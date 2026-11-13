"""Main campaign dispatcher orchestrator: scheduling, concurrency limits, and reconciliation."""
from __future__ import annotations

import asyncio
import csv
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .config import IST, SimClock, SystemConfig
from .db import Database
from .telephony_client import TelephonyClient

logger = logging.getLogger("dispatcher")


class CampaignDispatcher:
    def __init__(
        self,
        db: Database,
        cfg: SystemConfig,
        telephony: TelephonyClient,
        callback_url: str = "http://127.0.0.1:8200/webhooks/telephony",
        output_csv_path: str = "final_state.csv",
        wake_event: Optional[asyncio.Event] = None,
    ):
        self.db = db
        self.cfg = cfg
        self.telephony = telephony
        self.callback_url = callback_url
        self.output_csv_path = output_csv_path
        self.wake_event = wake_event or asyncio.Event()
        self.clock: Optional[SimClock] = None
        self._running = False
        self._dispatch_task: Optional[asyncio.Task] = None
        self._reconcile_task: Optional[asyncio.Task] = None
        self._clock_sync_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Connect to provider clock and seed database if necessary."""
        logger.info("Initializing dispatcher: syncing clock with provider...")
        clock_data = None
        for attempt in range(30):
            try:
                clock_data = await self.telephony.get_clock()
                break
            except Exception as e:
                logger.warning("Waiting for telephony provider at %s (%s)...", self.telephony.base_url, e)
                await asyncio.sleep(1.0)

        if not clock_data:
            raise RuntimeError(f"Could not connect to telephony provider at {self.telephony.base_url}")

        sim_start = datetime.fromisoformat(clock_data["sim_now"])
        speed = float(clock_data.get("speed", 600.0))
        real_epoch = float(clock_data.get("real_epoch", 0.0))
        sim_epoch = datetime.fromisoformat(clock_data["sim_epoch"])

        self.clock = SimClock(sim_epoch=sim_epoch, real_epoch=real_epoch, speed=speed)
        logger.info("Clock synchronized: sim_now=%s, speed=%.1fx", self.clock.now().isoformat(), speed)

        # Load fixtures into SQLite if first run
        self.db.load_fixtures_if_empty(self.cfg)

        # Run crash recovery reconciliation on any lingering dialing or active calls
        await self.recover_in_flight_calls()

    async def recover_in_flight_calls(self):
        """Reconcile in-flight calls after process restart or crash."""
        logger.info("Checking for lingering dialing or active calls from previous run...")

        # 1. Check stuck dialing calls
        dialing_calls = self.db.get_calls_by_state("dialing")
        for call in dialing_calls:
            rid = call["request_id"]
            logger.info("Recovering dialing call request_id=%s...", rid)
            status_code, data = await self.telephony.get_call_by_request_id(rid)
            if status_code == 200 and data:
                call_sid = data["call_sid"]
                if data.get("state") == "ended" and "result" in data:
                    res = data["result"]
                    self.db.process_call_result(
                        call_sid=call_sid,
                        request_id=rid,
                        tenant_id=res["tenant_id"],
                        campaign_id=res["campaign_id"],
                        lead_id=res["lead_id"],
                        phone=res["phone"],
                        status=res["status"],
                        disposition=res.get("disposition"),
                        callback_at=res.get("callback_at"),
                        started_at=res.get("started_at"),
                        ended_at=res.get("ended_at"),
                        duration_sec=res.get("duration_sec", 0),
                        cfg=self.cfg,
                    )
                else:
                    self.db.record_call_accepted(rid, call_sid)
            elif status_code == 404:
                # Call was never accepted by provider
                self.db.record_call_failed_to_place(rid)

        # 2. Check active calls
        active_calls = self.db.get_calls_by_state("active")
        for call in active_calls:
            sid = call.get("call_sid")
            if not sid:
                continue
            status_code, data = await self.telephony.get_call_by_sid(sid)
            if status_code == 200 and data and data.get("state") == "ended" and "result" in data:
                res = data["result"]
                self.db.process_call_result(
                    call_sid=sid,
                    request_id=call["request_id"],
                    tenant_id=res["tenant_id"],
                    campaign_id=res["campaign_id"],
                    lead_id=res["lead_id"],
                    phone=res["phone"],
                    status=res["status"],
                    disposition=res.get("disposition"),
                    callback_at=res.get("callback_at"),
                    started_at=res.get("started_at"),
                    ended_at=res.get("ended_at"),
                    duration_sec=res.get("duration_sec", 0),
                    cfg=self.cfg,
                )

    async def start(self):
        """Start background loops."""
        self._running = True
        self._dispatch_task = asyncio.create_task(self._run_dispatch_loop())
        self._reconcile_task = asyncio.create_task(self._run_reconcile_loop())
        self._clock_sync_task = asyncio.create_task(self._run_clock_sync_loop())

    async def stop(self):
        """Stop background loops."""
        self._running = False
        if self._dispatch_task:
            self._dispatch_task.cancel()
        if self._reconcile_task:
            self._reconcile_task.cancel()
        if self._clock_sync_task:
            self._clock_sync_task.cancel()
        await self.telephony.close()

    async def _run_clock_sync_loop(self):
        """Periodically sync clock with provider to prevent drift."""
        while self._running:
            try:
                await asyncio.sleep(10.0)
                data = await self.telephony.get_clock()
                sim_epoch = datetime.fromisoformat(data["sim_epoch"])
                real_epoch = float(data["real_epoch"])
                speed = float(data.get("speed", 600.0))
                self.clock.update_from_provider(sim_epoch, real_epoch, speed)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Clock sync check failed: %s", e)

    async def _run_dispatch_loop(self):
        """Continuous dispatch loop: selects eligible leads and dispatches calls."""
        while self._running:
            try:
                if self.db.is_campaign_complete():
                    logger.info("All leads reached final states. Writing final_state.csv...")
                    self.write_final_state_csv()
                    break

                sim_now = self.clock.now()
                # Atomically select and reserve leads
                reserved_leads = self.db.select_and_reserve_leads(sim_now, self.cfg)

                if reserved_leads:
                    # Dispatch reserved calls concurrently
                    for lead_info in reserved_leads:
                        asyncio.create_task(self._dispatch_single_call(lead_info))

                # Sleep until woken or next tick
                # 0.05 real seconds = 30-60 sim seconds at 600x-1200x
                try:
                    await asyncio.wait_for(self.wake_event.wait(), timeout=0.05)
                    self.wake_event.clear()
                except asyncio.TimeoutError:
                    pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in dispatch loop: %s", e, exc_info=True)
                await asyncio.sleep(0.1)

    async def _dispatch_single_call(self, lead_info: Dict[str, Any]):
        """Place a single reserved call and handle all provider response scenarios."""
        rid = lead_info["request_id"]
        cid = lead_info["campaign_id"]
        lid = lead_info["lead_id"]
        phone = lead_info["phone"]

        status_code, data = await self.telephony.place_call(
            request_id=rid,
            campaign_id=cid,
            lead_id=lid,
            phone=phone,
            callback_url=self.callback_url,
        )

        if status_code in (200, 201):
            call_sid = data.get("call_sid")
            self.db.record_call_accepted(rid, call_sid)
            # If outcome is already in data (e.g. on replay), check if ended
            if data.get("state") == "ended" and "result" in data:
                res = data["result"]
                self.db.process_call_result(
                    call_sid=call_sid,
                    request_id=rid,
                    tenant_id=res["tenant_id"],
                    campaign_id=cid,
                    lead_id=lid,
                    phone=phone,
                    status=res["status"],
                    disposition=res.get("disposition"),
                    callback_at=res.get("callback_at"),
                    started_at=res.get("started_at"),
                    ended_at=res.get("ended_at"),
                    duration_sec=res.get("duration_sec", 0),
                    cfg=self.cfg,
                )
        elif status_code == 503:
            logger.info("Provider returned 503 for %s:%s; call not placed. Will retry.", cid, lid)
            self.db.record_call_failed_to_place(rid)
            self.wake_event.set()
        elif status_code == 504:
            logger.warning("Provider returned 504 (outcome unknown) for %s:%s. Reconciling...", cid, lid)
            # Immediately query by request_id
            rec_code, rec_data = await self.telephony.get_call_by_request_id(rid)
            if rec_code == 200 and rec_data:
                call_sid = rec_data.get("call_sid")
                self.db.record_call_accepted(rid, call_sid)
                if rec_data.get("state") == "ended" and "result" in rec_data:
                    res = rec_data["result"]
                    self.db.process_call_result(
                        call_sid=call_sid,
                        request_id=rid,
                        tenant_id=res["tenant_id"],
                        campaign_id=cid,
                        lead_id=lid,
                        phone=phone,
                        status=res["status"],
                        disposition=res.get("disposition"),
                        callback_at=res.get("callback_at"),
                        started_at=res.get("started_at"),
                        ended_at=res.get("ended_at"),
                        duration_sec=res.get("duration_sec", 0),
                        cfg=self.cfg,
                    )
            elif rec_code == 404:
                self.db.record_call_failed_to_place(rid)
                self.wake_event.set()
            else:
                # Still unknown, reconcile loop will poll
                pass
        else:
            logger.error("Provider returned unexpected status %d for %s:%s: %s", status_code, cid, lid, data)
            if status_code in (409, 422, 429):
                # Provider rejected with business violation
                self.db.record_call_failed_to_place(rid)

    async def _run_reconcile_loop(self):
        """Periodic reconciliation to recover lost webhooks or stuck calls."""
        while self._running:
            try:
                await asyncio.sleep(0.5)

                # 1. Recover lost webhooks for active calls
                active_calls = self.db.get_calls_by_state("active")
                for call in active_calls:
                    sid = call.get("call_sid")
                    if not sid:
                        continue
                    st_code, data = await self.telephony.get_call_by_sid(sid)
                    if st_code == 200 and data and data.get("state") == "ended" and "result" in data:
                        res = data["result"]
                        logger.info("Reconciled ended call %s (lost webhook recovery)", sid)
                        self.db.process_call_result(
                            call_sid=sid,
                            request_id=call["request_id"],
                            tenant_id=res["tenant_id"],
                            campaign_id=res["campaign_id"],
                            lead_id=res["lead_id"],
                            phone=res["phone"],
                            status=res["status"],
                            disposition=res.get("disposition"),
                            callback_at=res.get("callback_at"),
                            started_at=res.get("started_at"),
                            ended_at=res.get("ended_at"),
                            duration_sec=res.get("duration_sec", 0),
                            cfg=self.cfg,
                        )
                        self.wake_event.set()

                # 2. Reconcile dialing calls stuck > 10s
                dialing_calls = self.db.get_stuck_dialing_calls(min_age_seconds=10.0)
                for call in dialing_calls:
                    rid = call["request_id"]
                    st_code, data = await self.telephony.get_call_by_request_id(rid)
                    if st_code == 200 and data:
                        call_sid = data.get("call_sid")
                        self.db.record_call_accepted(rid, call_sid)
                        if data.get("state") == "ended" and "result" in data:
                            res = data["result"]
                            self.db.process_call_result(
                                call_sid=call_sid,
                                request_id=rid,
                                tenant_id=res["tenant_id"],
                                campaign_id=res["campaign_id"],
                                lead_id=res["lead_id"],
                                phone=res["phone"],
                                status=res["status"],
                                disposition=res.get("disposition"),
                                callback_at=res.get("callback_at"),
                                started_at=res.get("started_at"),
                                ended_at=res.get("ended_at"),
                                duration_sec=res.get("duration_sec", 0),
                                cfg=self.cfg,
                            )
                        self.wake_event.set()
                    elif st_code == 404:
                        self.db.record_call_failed_to_place(rid)
                        self.wake_event.set()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in reconcile loop: %s", e)

    def write_final_state_csv(self):
        """Write final_state.csv sorted by campaign_id, lead_id."""
        records = self.db.get_final_states()
        fieldnames = ["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"]

        with open(self.output_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records:
                writer.writerow({
                    "campaign_id": r["campaign_id"],
                    "lead_id": r["lead_id"],
                    "final_state": r["final_state"],
                    "attempts": r["attempts"],
                    "last_status": r["last_status"],
                    "last_disposition": r["last_disposition"],
                })
        logger.info("Wrote %d records to %s", len(records), self.output_csv_path)

    async def wait_until_complete(self, poll_interval: float = 0.5):
        """Wait until all leads reach final states and output is written."""
        while self._running:
            if self.db.is_campaign_complete():
                self.write_final_state_csv()
                break
            await asyncio.sleep(poll_interval)
