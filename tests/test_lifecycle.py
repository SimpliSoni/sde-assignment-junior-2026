"""Unit tests for lead lifecycle transitions, terminal dispositions, DND propagation, and precedence."""
import tempfile
from datetime import datetime, timedelta
import pytest

from solution.config import (
    IST,
    CampaignConfig,
    RetryPolicy,
    SystemConfig,
    TenantConfig,
)
from solution.db import Database


def setup_db_with_leads(db: Database) -> SystemConfig:
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
    cfg.campaigns["c1"] = CampaignConfig(
        campaign_id="c1",
        tenant_id="t1",
        leads_file="leads1.csv",
        window_start="10:00",
        window_end="20:00",
        window_days=["thu", "fri"],
        max_attempts=3,
    )
    cfg.campaigns["c2"] = CampaignConfig(
        campaign_id="c2",
        tenant_id="t1",
        leads_file="leads2.csv",
        window_start="10:00",
        window_end="20:00",
        window_days=["thu", "fri"],
        max_attempts=3,
    )

    with db.transaction() as conn:
        conn.execute("INSERT INTO tenants VALUES ('t1', 5)")
        conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', 'leads1.csv', '10:00', '20:00', 'thu,fri', 3)")
        conn.execute("INSERT INTO campaigns VALUES ('c2', 't1', 'leads2.csv', '10:00', '20:00', 'thu,fri', 3)")

    return cfg


def test_terminal_disposition_completed():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db = Database(tmp.name)
        cfg = setup_db_with_leads(db)

        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts) VALUES ('c1', 'l1', 't1', '+919999999901', 'pending', 0)"
            )

        # Place call
        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        res = db.select_and_reserve_leads(sim_now, cfg)
        assert len(res) == 1
        rid = res[0]["request_id"]
        db.record_call_accepted(rid, "CA001")

        # Call ends with 'interested' (terminal)
        db.process_call_result(
            call_sid="CA001",
            request_id=rid,
            tenant_id="t1",
            campaign_id="c1",
            lead_id="l1",
            phone="+919999999901",
            status="completed",
            disposition="interested",
            callback_at=None,
            started_at=sim_now.isoformat(),
            ended_at=(sim_now + timedelta(seconds=120)).isoformat(),
            duration_sec=120,
            cfg=cfg,
        )

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'l1'").fetchone()
            assert lead["status"] == "completed"
            assert lead["attempts"] == 1
            assert lead["last_disposition"] == "interested"


def test_dnd_global_propagation_and_precedence():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db = Database(tmp.name)
        cfg = setup_db_with_leads(db)
        shared_phone = "+919876543210"

        with db.transaction() as conn:
            # Lead in Campaign 1
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts) VALUES ('c1', 'l_c1', 't1', ?, 'pending', 0)",
                (shared_phone,),
            )
            # Lead in Campaign 2 with SAME phone (pending)
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts) VALUES ('c2', 'l_c2_pending', 't1', ?, 'pending', 0)",
                (shared_phone,),
            )
            # Lead in Campaign 2 with SAME phone already completed
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts) VALUES ('c2', 'l_c2_completed', 't1', ?, 'completed', 1)",
                (shared_phone,),
            )
            # Lead in Campaign 2 with SAME phone already exhausted
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts) VALUES ('c2', 'l_c2_exhausted', 't1', ?, 'exhausted', 3)",
                (shared_phone,),
            )

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
        # Dial l_c1
        res = db.select_and_reserve_leads(sim_now, cfg)
        # l_c1 is reserved
        l_c1_res = [r for r in res if r["lead_id"] == "l_c1"][0]
        db.record_call_accepted(l_c1_res["request_id"], "CA_DND")

        # Call ends with DND
        db.process_call_result(
            call_sid="CA_DND",
            request_id=l_c1_res["request_id"],
            tenant_id="t1",
            campaign_id="c1",
            lead_id="l_c1",
            phone=shared_phone,
            status="completed",
            disposition="dnd",
            callback_at=None,
            started_at=sim_now.isoformat(),
            ended_at=(sim_now + timedelta(seconds=60)).isoformat(),
            duration_sec=60,
            cfg=cfg,
        )

        with db.get_connection() as conn:
            # l_c1 itself reached terminal DND -> status = completed
            l1 = conn.execute("SELECT status FROM leads WHERE lead_id = 'l_c1'").fetchone()
            assert l1["status"] == "completed"

            # l_c2_pending should be immediately blocked -> dnd_blocked
            l2_pend = conn.execute("SELECT status FROM leads WHERE lead_id = 'l_c2_pending'").fetchone()
            assert l2_pend["status"] == "dnd_blocked"

            # l_c2_completed was already completed -> remains completed (R9 precedence)
            l2_comp = conn.execute("SELECT status FROM leads WHERE lead_id = 'l_c2_completed'").fetchone()
            assert l2_comp["status"] == "completed"

            # l_c2_exhausted was already exhausted -> remains exhausted (R9 precedence)
            l2_exh = conn.execute("SELECT status FROM leads WHERE lead_id = 'l_c2_exhausted'").fetchone()
            assert l2_exh["status"] == "exhausted"


def test_retry_backoff_and_exhaustion():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db = Database(tmp.name)
        cfg = setup_db_with_leads(db)

        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, status, attempts) VALUES ('c1', 'l_retry', 't1', '+919999999999', 'pending', 0)"
            )

        sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)

        # Attempt 1: busy (60 min backoff)
        res1 = db.select_and_reserve_leads(sim_now, cfg)[0]
        db.record_call_accepted(res1["request_id"], "CA_R1")
        ended_1 = sim_now + timedelta(seconds=10)
        db.process_call_result(
            call_sid="CA_R1",
            request_id=res1["request_id"],
            tenant_id="t1",
            campaign_id="c1",
            lead_id="l_retry",
            phone="+919999999999",
            status="busy",
            disposition=None,
            callback_at=None,
            started_at=sim_now.isoformat(),
            ended_at=ended_1.isoformat(),
            duration_sec=0,
            cfg=cfg,
        )

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'l_retry'").fetchone()
            assert lead["status"] == "retry_scheduled"
            assert lead["attempts"] == 1
            # next eligible should be ended_1 + 60 minutes
            expected_next = ended_1 + timedelta(minutes=60)
            assert lead["next_eligible_at"] == expected_next.isoformat()

        # At 30 min later: lead should NOT be eligible
        sim_mid = sim_now + timedelta(minutes=30)
        res_mid = db.select_and_reserve_leads(sim_mid, cfg)
        assert len(res_mid) == 0

        # At 61 min later: lead IS eligible for Attempt 2
        sim_ready = ended_1 + timedelta(minutes=61)
        res2 = db.select_and_reserve_leads(sim_ready, cfg)[0]
        assert res2["lead_id"] == "l_retry"
        db.record_call_accepted(res2["request_id"], "CA_R2")
        ended_2 = sim_ready + timedelta(seconds=5)
        # Attempt 2 fails with 'failed' (15 min backoff)
        db.process_call_result(
            call_sid="CA_R2",
            request_id=res2["request_id"],
            tenant_id="t1",
            campaign_id="c1",
            lead_id="l_retry",
            phone="+919999999999",
            status="failed",
            disposition=None,
            callback_at=None,
            started_at=sim_ready.isoformat(),
            ended_at=ended_2.isoformat(),
            duration_sec=0,
            cfg=cfg,
        )

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'l_retry'").fetchone()
            assert lead["status"] == "retry_scheduled"
            assert lead["attempts"] == 2
            expected_next2 = ended_2 + timedelta(minutes=15)
            assert lead["next_eligible_at"] == expected_next2.isoformat()

        # Attempt 3: 20 min later
        sim_att3 = ended_2 + timedelta(minutes=20)
        res3 = db.select_and_reserve_leads(sim_att3, cfg)[0]
        assert res3["lead_id"] == "l_retry"
        db.record_call_accepted(res3["request_id"], "CA_R3")
        ended_3 = sim_att3 + timedelta(seconds=40)
        # Attempt 3: no_answer. Now attempts == 3 == max_attempts.
        db.process_call_result(
            call_sid="CA_R3",
            request_id=res3["request_id"],
            tenant_id="t1",
            campaign_id="c1",
            lead_id="l_retry",
            phone="+919999999999",
            status="no_answer",
            disposition=None,
            callback_at=None,
            started_at=sim_att3.isoformat(),
            ended_at=ended_3.isoformat(),
            duration_sec=0,
            cfg=cfg,
        )

        with db.get_connection() as conn:
            lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'l_retry'").fetchone()
            # Must be EXHAUSTED! Never dialed again.
            assert lead["status"] == "exhausted"
            assert lead["attempts"] == 3

