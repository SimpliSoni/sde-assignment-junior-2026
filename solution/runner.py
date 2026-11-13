"""Entry point and runner for the Campaign Dispatcher service."""
from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import sys

import uvicorn

from .config import SystemConfig
from .db import Database
from .dispatcher import CampaignDispatcher
from .telephony_client import TelephonyClient
from .webhook_server import create_webhook_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("runner")


def parse_args():
    parser = argparse.ArgumentParser(description="Agentic Universe Campaign Dispatcher")
    parser.add_argument(
        "--fixtures",
        default="assignment-b-workflow/fixtures/campaigns.json",
        help="Path to campaigns.json fixtures",
    )
    parser.add_argument(
        "--mock-url",
        default="http://127.0.0.1:8099",
        help="Mock telephony provider base URL",
    )
    parser.add_argument(
        "--webhook-host",
        default="127.0.0.1",
        help="Host to bind webhook receiver",
    )
    parser.add_argument(
        "--webhook-port",
        type=int,
        default=8200,
        help="Port for webhook receiver",
    )
    parser.add_argument(
        "--callback-url",
        default="http://127.0.0.1:8200/webhooks/telephony",
        help="Callback webhook URL sent to telephony provider",
    )
    parser.add_argument(
        "--db",
        default="solution/dispatcher.db",
        help="Path to SQLite database file",
    )
    parser.add_argument(
        "--output",
        default="final_state.csv",
        help="Path to output final_state.csv",
    )
    parser.add_argument(
        "--exit-on-complete",
        action="store_true",
        default=True,
        help="Exit automatically once all leads reach final state",
    )
    return parser.parse_args()


async def verify_against_provider(telephony: TelephonyClient, output_csv_path: str):
    """Fetch provider's admin report and compare final_state.csv with /admin/leads."""
    try:
        report = await telephony.get_admin_report()
        logger.info("=== Telephony Provider /admin/report ===")
        logger.info("Simulation Now: %s", report.get("sim_now"))
        logger.info("Total Calls: %s | Active Calls: %s", report.get("calls_total"), report.get("calls_active"))
        logger.info("Calls by status: %s", report.get("calls_by_status"))
        logger.info("Violation count: %d", report.get("violation_count", -1))
        logger.info("Violations by type: %s", report.get("violations_by_type"))
        logger.info("Max Concurrency Seen: %s", report.get("max_concurrency_seen"))
        logger.info("Callback Lateness (minutes): %s", report.get("callback_lateness_minutes"))
        logger.info("Webhook stats: %s", report.get("webhooks"))

        admin_leads = await telephony.get_admin_leads()
        admin_map = {(l["campaign_id"], l["lead_id"]): l for l in admin_leads}

        discrepancies = []
        with open(output_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row["campaign_id"], row["lead_id"])
                expected = admin_map.get(key)
                if not expected:
                    discrepancies.append(f"Lead {key} not found in provider")
                    continue
                if row["final_state"] != expected["expected_state"]:
                    discrepancies.append(
                        f"Lead {key}: state mismatch {row['final_state']} != {expected['expected_state']}"
                    )
                if int(row["attempts"]) != int(expected["attempts"]):
                    discrepancies.append(
                        f"Lead {key}: attempts mismatch {row['attempts']} != {expected['attempts']}"
                    )

        if discrepancies:
            logger.error("Found %d discrepancies between final_state.csv and provider /admin/leads!", len(discrepancies))
            for d in discrepancies[:10]:
                logger.error("  %s", d)
        else:
            logger.info("PERFECT MATCH! final_state.csv exactly matches provider's expected states for all %d leads.", len(admin_map))

    except Exception as e:
        logger.warning("Could not run verification against provider: %s", e)


async def main_async():
    args = parse_args()

    cfg = SystemConfig.load_from_file(args.fixtures)
    db = Database(args.db)
    telephony = TelephonyClient(base_url=args.mock_url)
    wake_event = asyncio.Event()

    dispatcher = CampaignDispatcher(
        db=db,
        cfg=cfg,
        telephony=telephony,
        callback_url=args.callback_url,
        output_csv_path=args.output,
        wake_event=wake_event,
    )

    app = create_webhook_app(
        db=db,
        cfg=cfg,
        wake_event=wake_event,
        clock_fn=lambda: dispatcher.clock.now() if dispatcher.clock else None,
    )

    uvicorn_config = uvicorn.Config(
        app=app,
        host=args.webhook_host,
        port=args.webhook_port,
        log_level="warning",
    )
    server = uvicorn.Server(uvicorn_config)

    # Start webhook server in background
    server_task = asyncio.create_task(server.serve())

    # Wait briefly for webhook server to be listening
    await asyncio.sleep(0.5)

    try:
        await dispatcher.initialize()
        await dispatcher.start()

        if args.exit_on_complete:
            logger.info("Dispatcher running. Waiting for all leads to reach final state...")
            await dispatcher.wait_until_complete()
            logger.info("Campaign completed successfully! Verifying results against provider...")
            await verify_against_provider(telephony, args.output)
            logger.info("Shutting down service...")
        else:
            logger.info("Dispatcher running in daemon mode. Press Ctrl+C to terminate.")
            while True:
                await asyncio.sleep(1.0)

    finally:
        await dispatcher.stop()
        server.should_exit = True
        await server_task


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Process terminated by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()

