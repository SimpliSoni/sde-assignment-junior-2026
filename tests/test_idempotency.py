"""Unit tests for webhook idempotency, HMAC signature validation, and race conditions."""
import hashlib
import hmac
import json
import tempfile
from datetime import datetime
import pytest
from fastapi.testclient import TestClient

from solution.config import (
    IST,
    CampaignConfig,
    RetryPolicy,
    SystemConfig,
    TenantConfig,
)
from solution.db import Database
from solution.webhook_server import create_webhook_app


def setup_test_app():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db = Database(tmp.name)
    secret = "test_webhook_secret_key"
    cfg = SystemConfig(
        sim_start=datetime(2026, 11, 12, 17, 30, tzinfo=IST),
        timezone_name="Asia/Kolkata",
        regulatory_start="09:00",
        regulatory_end="21:00",
        window_grace_seconds=120,
        retry_policy=RetryPolicy(60, 60, 15),
        webhook_secret=secret,
    )
    cfg.tenants["t1"] = TenantConfig(tenant_id="t1", max_concurrent_calls=5)
    cfg.campaigns["c1"] = CampaignConfig("c1", "t1", "l.csv", "10:00", "20:00", ["thu"], 3)

    with db.transaction() as conn:
        conn.execute("INSERT INTO tenants VALUES ('t1', 5)")
        conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', 'l.csv', '10:00', '20:00', 'thu', 3)")
        conn.execute(
            "INSERT INTO leads VALUES ('c1', 'lead_1', 't1', '+919999999999', 'Test', 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

    app = create_webhook_app(db=db, cfg=cfg)
    client = TestClient(app)
    return db, cfg, client, secret


def sign_body(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_webhook_signature_validation():
    db, cfg, client, secret = setup_test_app()

    payload = {
        "event": "call.completed",
        "call_sid": "CA_TEST_1",
        "request_id": "req_1",
        "tenant_id": "t1",
        "campaign_id": "c1",
        "lead_id": "lead_1",
        "phone": "+919999999999",
        "status": "completed",
        "disposition": "interested",
        "ended_at": "2026-11-12T17:35:00+05:30",
        "duration_sec": 60,
    }
    raw_bytes = json.dumps(payload).encode()

    # 1. Missing signature
    resp = client.post("/webhooks/telephony", content=raw_bytes)
    assert resp.status_code == 401

    # 2. Invalid signature
    resp = client.post("/webhooks/telephony", content=raw_bytes, headers={"X-AU-Signature": "invalid_sig"})
    assert resp.status_code == 401

    # 3. Valid signature
    sig = sign_body(raw_bytes, secret)
    resp = client.post("/webhooks/telephony", content=raw_bytes, headers={"X-AU-Signature": sig})
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_duplicate_webhook_delivery():
    db, cfg, client, secret = setup_test_app()

    payload = {
        "event": "call.completed",
        "call_sid": "CA_DUP",
        "request_id": "req_dup",
        "tenant_id": "t1",
        "campaign_id": "c1",
        "lead_id": "lead_1",
        "phone": "+919999999999",
        "status": "completed",
        "disposition": "interested",
        "ended_at": "2026-11-12T17:35:00+05:30",
        "duration_sec": 60,
    }
    raw_bytes = json.dumps(payload).encode()
    sig = sign_body(raw_bytes, secret)

    # 1st delivery
    resp1 = client.post("/webhooks/telephony", content=raw_bytes, headers={"X-AU-Signature": sig})
    assert resp1.status_code == 200
    assert resp1.json()["newly_processed"] is True

    # 2nd duplicate delivery
    resp2 = client.post("/webhooks/telephony", content=raw_bytes, headers={"X-AU-Signature": sig})
    assert resp2.status_code == 200
    assert resp2.json()["newly_processed"] is False

    # Check lead attempts count is NOT double-counted
    with db.get_connection() as conn:
        lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'lead_1'").fetchone()
        assert lead["attempts"] == 1
        assert lead["status"] == "completed"


def test_out_of_order_webhook_before_call_accepted():
    db, cfg, client, secret = setup_test_app()

    # Dispatcher placed call in 'dialing' state
    sim_now = datetime(2026, 11, 12, 17, 30, tzinfo=IST)
    res = db.select_and_reserve_leads(sim_now, cfg)[0]
    rid = res["request_id"]

    # Carrier finishes very fast and webhook arrives BEFORE record_call_accepted is called!
    payload = {
        "event": "call.completed",
        "call_sid": "CA_FAST",
        "request_id": rid,
        "tenant_id": "t1",
        "campaign_id": "c1",
        "lead_id": "lead_1",
        "phone": "+919999999999",
        "status": "completed",
        "disposition": "interested",
        "ended_at": "2026-11-12T17:31:00+05:30",
        "duration_sec": 60,
    }
    raw_bytes = json.dumps(payload).encode()
    sig = sign_body(raw_bytes, secret)

    wh_resp = client.post("/webhooks/telephony", content=raw_bytes, headers={"X-AU-Signature": sig})
    assert wh_resp.status_code == 200

    # Now the 201 response returns in the dispatcher thread:
    db.record_call_accepted(rid, "CA_FAST")

    # Lead must still be completed and attempts must be 1 (not 2!)
    with db.get_connection() as conn:
        lead = conn.execute("SELECT * FROM leads WHERE lead_id = 'lead_1'").fetchone()
        assert lead["status"] == "completed"
        assert lead["attempts"] == 1
        call = conn.execute("SELECT * FROM calls WHERE request_id = ?", (rid,)).fetchone()
        assert call["state"] == "ended"

