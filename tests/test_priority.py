"""Unit tests for dispatch priority ordering: Callbacks > Retries > Fresh Leads."""
import tempfile
from datetime import datetime
import pytest

from solution.config import (
    IST,
    CampaignConfig,
    RetryPolicy,
    SystemConfig,
    TenantConfig,
)
from solution.db import Database


def make_test_config(db_path: str) -> SystemConfig:
    cfg = SystemConfig(
        sim_start=datetime(2026, 11, 12, 17, 30, tzinfo=IST),
        timezone_name="Asia/Kolkata",
        regulatory_start="09:00",
        regulatory_end="21:00",
        window_grace_seconds=120,
        retry_policy=RetryPolicy(60, 60, 15),
        webhook_secret="test_secret",
    )
    cfg.tenants["t1"] = TenantConfig(tenant_id="t1", max_concurrent_calls=10)
    cfg.campaigns["c1"] = CampaignConfig(
        campaign_id="c1",
        tenant_id="t1",
        leads_file="leads.csv",
        window_start="10:00",
        window_end="20:00",
        window_days=["thu", "fri"],
        max_attempts=3,
    )
    return cfg


def test_priority_ordering_callbacks_retries_fresh():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db = Database(tmp.name)
        cfg = make_test_config(tmp.name)

        # Populate tables manually
        with db.transaction() as conn:
            conn.execute("INSERT INTO tenants VALUES ('t1', 10)")
            conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', 'leads.csv', '10:00', '20:00', 'thu,fri', 3)")

            # Lead 1: Fresh lead (pending)
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts) VALUES ('c1', 'lead_fresh', 't1', '+919000000001', 'pending', 0)"
            )
            # Lead 2: Retry lead (retry_scheduled, due at 17:00)
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts, next_eligible_at) VALUES ('c1', 'lead_retry', 't1', '+919000000002', 'retry_scheduled', 1, '2026-11-12T17:00:00+05:30')"
            )
            # Lead 3: Callback lead (callback_scheduled, due at 17:15)
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts, next_eligible_at) VALUES ('c1', 'lead_callback', 't1', '+919000000003', 'callback_scheduled', 1, '2026-11-12T17:15:00+05:30')"
            )
            # Lead 4: Future callback (callback_scheduled, due at 18:00 - not yet due at 17:30)
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts, next_eligible_at) VALUES ('c1', 'lead_future_cb', 't1', '+919000000004', 'callback_scheduled', 1, '2026-11-12T18:00:00+05:30')"
            )

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        selected = db.select_and_reserve_leads(sim_now, cfg)

        # 3 leads should be eligible (future callback is excluded)
        assert len(selected) == 3
        # 1st must be the due callback
        assert selected[0]["lead_id"] == "lead_callback"
        # 2nd must be the retry
        assert selected[1]["lead_id"] == "lead_retry"
        # 3rd must be the fresh lead
        assert selected[2]["lead_id"] == "lead_fresh"


def test_priority_earlier_callback_chosen_first():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db = Database(tmp.name)
        cfg = make_test_config(tmp.name)

        with db.transaction() as conn:
            conn.execute("INSERT INTO tenants VALUES ('t1', 10)")
            conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', 'leads.csv', '10:00', '20:00', 'thu,fri', 3)")

            # Two due callbacks: one due at 16:30, one due at 17:00
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts, next_eligible_at) VALUES ('c1', 'cb_late', 't1', '+919000000010', 'callback_scheduled', 1, '2026-11-12T17:00:00+05:30')"
            )
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts, next_eligible_at) VALUES ('c1', 'cb_early', 't1', '+919000000020', 'callback_scheduled', 1, '2026-11-12T16:30:00+05:30')"
            )

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        selected = db.select_and_reserve_leads(sim_now, cfg)

        assert len(selected) == 2
        # Earlier callback must be picked first!
        assert selected[0]["lead_id"] == "cb_early"
        assert selected[1]["lead_id"] == "cb_late"

