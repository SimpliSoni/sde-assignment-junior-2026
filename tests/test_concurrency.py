"""Unit tests for concurrency limits (R2) and phone exclusivity (R3)."""
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


def test_tenant_concurrency_across_campaigns():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
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
        # Tenant limit is 2
        cfg.tenants["t1"] = TenantConfig(tenant_id="t1", max_concurrent_calls=2)
        # 2 campaigns under tenant t1
        cfg.campaigns["c1"] = CampaignConfig("c1", "t1", "l1.csv", "10:00", "20:00", ["thu"], 3)
        cfg.campaigns["c2"] = CampaignConfig("c2", "t1", "l2.csv", "10:00", "20:00", ["thu"], 3)

        with db.transaction() as conn:
            conn.execute("INSERT INTO tenants VALUES ('t1', 2)")
            conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', 'l1.csv', '10:00', '20:00', 'thu', 3)")
            conn.execute("INSERT INTO campaigns VALUES ('c2', 't1', 'l2.csv', '10:00', '20:00', 'thu', 3)")

            conn.execute("INSERT INTO leads VALUES ('c1', 'l1', 't1', '+919000000001', 'A', 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL)")
            conn.execute("INSERT INTO leads VALUES ('c1', 'l2', 't1', '+919000000002', 'B', 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL)")
            conn.execute("INSERT INTO leads VALUES ('c2', 'l3', 't1', '+919000000003', 'C', 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL)")

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)

        # Batch 1: should reserve exactly 2 leads (the max limit for tenant t1)
        res1 = db.select_and_reserve_leads(sim_now, cfg)
        assert len(res1) == 2

        # Batch 2 immediately after: should reserve 0 leads because tenant limit 2 is reached
        res2 = db.select_and_reserve_leads(sim_now, cfg)
        assert len(res2) == 0


def test_one_call_per_phone_across_campaigns():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
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
        # Tenant allows 10 calls
        cfg.tenants["t1"] = TenantConfig(tenant_id="t1", max_concurrent_calls=10)
        cfg.campaigns["c1"] = CampaignConfig("c1", "t1", "l1.csv", "10:00", "20:00", ["thu"], 3)
        cfg.campaigns["c2"] = CampaignConfig("c2", "t1", "l2.csv", "10:00", "20:00", ["thu"], 3)

        shared_phone = "+919876543210"

        with db.transaction() as conn:
            conn.execute("INSERT INTO tenants VALUES ('t1', 10)")
            conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', 'l1.csv', '10:00', '20:00', 'thu', 3)")
            conn.execute("INSERT INTO campaigns VALUES ('c2', 't1', 'l2.csv', '10:00', '20:00', 'thu', 3)")

            # Two leads in different campaigns sharing the SAME phone
            conn.execute("INSERT INTO leads VALUES ('c1', 'lead_A', 't1', ?, 'A', 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL)", (shared_phone,))
            conn.execute("INSERT INTO leads VALUES ('c2', 'lead_B', 't1', ?, 'B', 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL)", (shared_phone,))

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)

        # In a single reservation call: only ONE lead with that phone can be selected!
        res = db.select_and_reserve_leads(sim_now, cfg)
        assert len(res) == 1
        assert res[0]["phone"] == shared_phone

        # While call is active, second lead cannot be dialed
        db.record_call_accepted(res[0]["request_id"], "CA_SHARED")
        res_active = db.select_and_reserve_leads(sim_now, cfg)
        assert len(res_active) == 0

