"""Unit tests for recovery from carrier failures (503, ambiguous 504) and lost webhooks."""
import asyncio
import tempfile
from datetime import datetime, timedelta
import pytest
from unittest.mock import AsyncMock

from solution.config import (
    IST,
    CampaignConfig,
    RetryPolicy,
    SystemConfig,
    TenantConfig,
)
from solution.db import Database
from solution.dispatcher import CampaignDispatcher
from solution.telephony_client import TelephonyClient


def setup_dispatcher():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db = Database(tmp.name)
    cfg = SystemConfig(
        sim_start=datetime(2026, 11, 12, 17, 30, tzinfo=IST),
        timezone_name="Asia/Kolkata",
        regulatory_start="09:00",
        regulatory_end="21:00",
        window_grace_seconds=120,
        retry_policy=RetryPolicy(60, 60, 15),
        webhook_secret="test_secret",
    )
    cfg.tenants["t1"] = TenantConfig(tenant_id="t1", max_concurrent_calls=5)
    cfg.campaigns["c1"] = CampaignConfig("c1", "t1", "l.csv", "10:00", "20:00", ["thu"], 3)

    with db.transaction() as conn:
        conn.execute("INSERT INTO tenants VALUES ('t1', 5)")
        conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', 'l.csv', '10:00', '20:00', 'thu', 3)")
        conn.execute(
            "INSERT INTO leads VALUES ('c1', 'lead_rec', 't1', '+919999999999', 'Rec', 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

    telephony = TelephonyClient()
    dispatcher = CampaignDispatcher(db=db, cfg=cfg, telephony=telephony)
    return db, cfg, dispatcher, telephony


def test_503_transient_failure_handling():
    async def _run():
        db, cfg, dispatcher, telephony = setup_dispatcher()

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        res = db.select_and_reserve_leads(sim_now, cfg)[0]
        rid = res["request_id"]

        # Mock telephony returning 503
        telephony.place_call = AsyncMock(return_value=(503, {"error": "service_unavailable"}))

        await dispatcher._dispatch_single_call(res)

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'lead_rec'").fetchone()
            # Lead status must revert to pending and attempts must remain 0 (R4)
            assert lead["status"] == "pending"
            assert lead["attempts"] == 0
            call = conn.execute("SELECT * FROM calls WHERE request_id = ?", (rid,)).fetchone()
            assert call["state"] == "failed_to_place"

    asyncio.run(_run())


def test_504_ambiguous_failure_resolved_as_accepted():
    async def _run():
        db, cfg, dispatcher, telephony = setup_dispatcher()

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        res = db.select_and_reserve_leads(sim_now, cfg)[0]
        rid = res["request_id"]

        # Mock place_call returning 504 timeout
        telephony.place_call = AsyncMock(return_value=(504, {"error": "upstream_timeout"}))
        # Mock subsequent reconciliation via get_call_by_request_id returning 200 accepted
        telephony.get_call_by_request_id = AsyncMock(
            return_value=(200, {"call_sid": "CA_RECOVERED_1", "state": "active"})
        )

        await dispatcher._dispatch_single_call(res)

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'lead_rec'").fetchone()
            # Reconciled as active!
            assert lead["status"] == "active"
            assert lead["attempts"] == 1
            assert lead["last_call_sid"] == "CA_RECOVERED_1"

    asyncio.run(_run())


def test_504_ambiguous_failure_resolved_as_not_placed():
    async def _run():
        db, cfg, dispatcher, telephony = setup_dispatcher()

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        res = db.select_and_reserve_leads(sim_now, cfg)[0]
        rid = res["request_id"]

        # Mock place_call returning 504
        telephony.place_call = AsyncMock(return_value=(504, {"error": "upstream_timeout"}))
        # Provider says 404: call never reached provider
        telephony.get_call_by_request_id = AsyncMock(return_value=(404, None))

        await dispatcher._dispatch_single_call(res)

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'lead_rec'").fetchone()
            # Reset to pending, attempts = 0
            assert lead["status"] == "pending"
            assert lead["attempts"] == 0

    asyncio.run(_run())


def test_lost_webhook_recovery_via_polling():
    async def _run():
        db, cfg, dispatcher, telephony = setup_dispatcher()

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        res = db.select_and_reserve_leads(sim_now, cfg)[0]
        rid = res["request_id"]
        db.record_call_accepted(rid, "CA_LOST_WH")

        # Call is currently active in DB. Carrier ended the call, but webhook was dropped!
        telephony.get_call_by_sid = AsyncMock(
            return_value=(
                200,
                {
                    "call_sid": "CA_LOST_WH",
                    "request_id": rid,
                    "state": "ended",
                    "result": {
                        "tenant_id": "t1",
                        "campaign_id": "c1",
                        "lead_id": "lead_rec",
                        "phone": "+919999999999",
                        "status": "completed",
                        "disposition": "interested",
                        "ended_at": (sim_now + timedelta(seconds=120)).isoformat(),
                        "duration_sec": 120,
                    },
                },
            )
        )

        # Run one pass of recover_in_flight_calls / reconcile
        await dispatcher.recover_in_flight_calls()

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'lead_rec'").fetchone()
            assert lead["status"] == "completed"
            assert lead["last_disposition"] == "interested"
            call = conn.execute("SELECT * FROM calls WHERE call_sid = 'CA_LOST_WH'").fetchone()
            assert call["state"] == "ended"

    asyncio.run(_run())
