"""SQLite database layer for durable campaign state, idempotency, and ACID transitions."""
from __future__ import annotations

import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

from .config import (
    IST,
    TERMINAL_DISPOSITIONS,
    CampaignConfig,
    RetryPolicy,
    SystemConfig,
    is_in_calling_window,
)


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_schema()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            yield conn
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise
        finally:
            conn.close()

    def _init_schema(self):
        with self.get_connection() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id TEXT PRIMARY KEY,
                max_concurrent_calls INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS campaigns (
                campaign_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                leads_file TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                window_days TEXT NOT NULL,
                max_attempts INTEGER NOT NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
            );

            CREATE TABLE IF NOT EXISTS leads (
                campaign_id TEXT NOT NULL,
                lead_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                customer_name TEXT,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_eligible_at TEXT,
                last_call_sid TEXT,
                last_request_id TEXT,
                last_status TEXT,
                last_disposition TEXT,
                last_ended_at TEXT,
                PRIMARY KEY (campaign_id, lead_id),
                FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
                FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
            );

            CREATE INDEX IF NOT EXISTS idx_leads_status_order ON leads(status, next_eligible_at);
            CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
            CREATE INDEX IF NOT EXISTS idx_leads_tenant ON leads(tenant_id);

            CREATE TABLE IF NOT EXISTS calls (
                request_id TEXT PRIMARY KEY,
                call_sid TEXT,
                tenant_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                lead_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                state TEXT NOT NULL, -- 'dialing', 'active', 'ended', 'failed_to_place'
                created_at TEXT NOT NULL,
                real_created_at REAL,
                started_at TEXT,
                ended_at TEXT,
                status TEXT,
                disposition TEXT,
                callback_at TEXT,
                duration_sec INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_calls_call_sid ON calls(call_sid);
            CREATE INDEX IF NOT EXISTS idx_calls_state ON calls(state);
            CREATE INDEX IF NOT EXISTS idx_calls_tenant_state ON calls(tenant_id, state);
            CREATE INDEX IF NOT EXISTS idx_calls_phone_state ON calls(phone, state);

            CREATE TABLE IF NOT EXISTS dnd_registry (
                phone TEXT PRIMARY KEY,
                marked_at TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                lead_id TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """)

    def load_fixtures_if_empty(self, cfg: SystemConfig):
        with self.transaction() as conn:
            cursor = conn.execute("SELECT COUNT(*) as count FROM campaigns")
            if cursor.fetchone()["count"] > 0:
                return  # already loaded

            for t in cfg.tenants.values():
                conn.execute(
                    "INSERT OR REPLACE INTO tenants (tenant_id, max_concurrent_calls) VALUES (?, ?)",
                    (t.tenant_id, t.max_concurrent_calls),
                )

            for c in cfg.campaigns.values():
                conn.execute(
                    """INSERT OR REPLACE INTO campaigns 
                       (campaign_id, tenant_id, leads_file, window_start, window_end, window_days, max_attempts) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        c.campaign_id,
                        c.tenant_id,
                        c.leads_file,
                        c.window_start,
                        c.window_end,
                        ",".join(c.window_days),
                        c.max_attempts,
                    ),
                )
                leads = cfg.load_leads(c.campaign_id)
                for l in leads:
                    conn.execute(
                        """INSERT OR IGNORE INTO leads 
                           (campaign_id, lead_id, tenant_id, phone, customer_name, status, attempts)
                           VALUES (?, ?, ?, ?, ?, 'pending', 0)""",
                        (
                            l["campaign_id"],
                            l["lead_id"],
                            l["tenant_id"],
                            l["phone"],
                            l["customer_name"],
                        ),
                    )

    def get_active_tenant_counts(self, conn: Optional[sqlite3.Connection] = None) -> Dict[str, int]:
        c = conn or self.get_connection()
        try:
            rows = c.execute(
                "SELECT tenant_id, COUNT(*) as count FROM calls WHERE state IN ('dialing', 'active') GROUP BY tenant_id"
            ).fetchall()
            return {r["tenant_id"]: r["count"] for r in rows}
        finally:
            if conn is None:
                c.close()

    def get_active_phones(self, conn: Optional[sqlite3.Connection] = None) -> Set[str]:
        c = conn or self.get_connection()
        try:
            rows = c.execute(
                "SELECT DISTINCT phone FROM calls WHERE state IN ('dialing', 'active')"
            ).fetchall()
            return {r["phone"] for r in rows}
        finally:
            if conn is None:
                c.close()

    def get_dnd_phones(self, conn: Optional[sqlite3.Connection] = None) -> Set[str]:
        c = conn or self.get_connection()
        try:
            rows = c.execute("SELECT phone FROM dnd_registry").fetchall()
            return {r["phone"] for r in rows}
        finally:
            if conn is None:
                c.close()

    def select_and_reserve_leads(
        self,
        sim_now: datetime,
        cfg: SystemConfig,
    ) -> List[Dict[str, Any]]:
        """Atomically find eligible leads adhering to R1-R8 and reserve them in 'dialing' state."""
        reserved = []
        with self.transaction() as conn:
            # 1. First enforce DND propagation on any pending leads with DND phone
            conn.execute("""
                UPDATE leads 
                SET status = 'dnd_blocked'
                WHERE phone IN (SELECT phone FROM dnd_registry)
                  AND status NOT IN ('completed', 'exhausted', 'dnd_blocked', 'dialing', 'active')
            """)

            active_tenants = self.get_active_tenant_counts(conn)
            active_phones = self.get_active_phones(conn)
            dnd_phones = self.get_dnd_phones(conn)

            # Query all eligible candidate leads across tenants
            rows = conn.execute("""
                SELECT l.campaign_id, l.lead_id, l.tenant_id, l.phone, l.status, l.attempts,
                       l.next_eligible_at, c.window_start, c.window_end, c.window_days, c.max_attempts,
                       t.max_concurrent_calls
                FROM leads l
                JOIN campaigns c ON l.campaign_id = c.campaign_id
                JOIN tenants t ON l.tenant_id = t.tenant_id
                WHERE l.status IN ('callback_scheduled', 'retry_scheduled', 'pending')
                  AND l.attempts < c.max_attempts
                  AND l.phone NOT IN (SELECT phone FROM dnd_registry)
                  AND l.phone NOT IN (SELECT DISTINCT phone FROM calls WHERE state IN ('dialing', 'active'))
                ORDER BY
                  CASE l.status
                    WHEN 'callback_scheduled' THEN 1
                    WHEN 'retry_scheduled' THEN 2
                    WHEN 'pending' THEN 3
                    ELSE 4
                  END,
                  l.next_eligible_at ASC,
                  l.lead_id ASC
            """).fetchall()

            for r in rows:
                tenant_id = r["tenant_id"]
                phone = r["phone"]
                max_concurrency = r["max_concurrent_calls"]
                current_active = active_tenants.get(tenant_id, 0)

                if current_active >= max_concurrency:
                    continue
                if phone in active_phones or phone in dnd_phones:
                    continue

                # Check window eligibility
                days = [d.strip() for d in r["window_days"].split(",")]
                in_win = is_in_calling_window(
                    sim_now,
                    r["window_start"],
                    r["window_end"],
                    days,
                    cfg.regulatory_start,
                    cfg.regulatory_end,
                    grace_s=0.0,
                )
                if not in_win:
                    continue

                # Check scheduling time (callback_at or retry backoff)
                if r["next_eligible_at"]:
                    next_el = datetime.fromisoformat(r["next_eligible_at"])
                    if sim_now < next_el:
                        continue

                # Lead is eligible! Reserve it atomically
                attempt_no = r["attempts"] + 1
                request_id = f"req_{r['campaign_id']}_{r['lead_id']}_{attempt_no}_{uuid.uuid4().hex[:8]}"

                conn.execute(
                    """INSERT INTO calls 
                       (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at, real_created_at)
                       VALUES (?, ?, ?, ?, ?, ?, 'dialing', ?, ?)""",
                    (
                        request_id,
                        tenant_id,
                        r["campaign_id"],
                        r["lead_id"],
                        phone,
                        attempt_no,
                        sim_now.isoformat(),
                        time.time(),
                    ),
                )

                conn.execute(
                    """UPDATE leads 
                       SET status = 'dialing', last_request_id = ?
                       WHERE campaign_id = ? AND lead_id = ?""",
                    (request_id, r["campaign_id"], r["lead_id"]),
                )

                # Update in-memory trackers for this batch
                active_tenants[tenant_id] = current_active + 1
                active_phones.add(phone)

                reserved.append({
                    "request_id": request_id,
                    "campaign_id": r["campaign_id"],
                    "lead_id": r["lead_id"],
                    "tenant_id": tenant_id,
                    "phone": phone,
                    "attempt_no": attempt_no,
                })

        return reserved

    def record_call_accepted(self, request_id: str, call_sid: str):
        """Update call state to active once provider returns 201 or 200."""
        with self.transaction() as conn:
            call = conn.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,)).fetchone()
            if not call:
                return

            if call["state"] == "ended":
                # Webhook already ended the call! Just ensure call_sid is set
                if not call["call_sid"]:
                    conn.execute("UPDATE calls SET call_sid = ? WHERE request_id = ?", (call_sid, request_id))
                return

            conn.execute(
                "UPDATE calls SET call_sid = ?, state = 'active' WHERE request_id = ?",
                (call_sid, request_id),
            )
            # Only increment attempts if the lead is currently in 'dialing'
            conn.execute(
                """UPDATE leads 
                   SET status = 'active', attempts = attempts + 1, last_call_sid = ?
                   WHERE campaign_id = ? AND lead_id = ? AND status = 'dialing'""",
                (call_sid, call["campaign_id"], call["lead_id"]),
            )

    def record_call_failed_to_place(self, request_id: str):
        """Reset lead and call state when provider returns 503 or call is confirmed not placed."""
        with self.transaction() as conn:
            call = conn.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,)).fetchone()
            if not call:
                return

            conn.execute("UPDATE calls SET state = 'failed_to_place' WHERE request_id = ?", (request_id,))

            lead = conn.execute(
                "SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?",
                (call["campaign_id"], call["lead_id"]),
            ).fetchone()

            if lead and lead["status"] == "dialing":
                # Revert to appropriate prior state without counting attempt
                if lead["last_disposition"] == "callback_requested":
                    new_st = "callback_scheduled"
                elif lead["attempts"] > 0:
                    new_st = "retry_scheduled"
                else:
                    new_st = "pending"

                conn.execute(
                    "UPDATE leads SET status = ? WHERE campaign_id = ? AND lead_id = ?",
                    (new_st, lead["campaign_id"], lead["lead_id"]),
                )

    def process_call_result(
        self,
        call_sid: str,
        request_id: str,
        tenant_id: str,
        campaign_id: str,
        lead_id: str,
        phone: str,
        status: str,
        disposition: Optional[str],
        callback_at: Optional[str],
        started_at: Optional[str],
        ended_at: Optional[str],
        duration_sec: int,
        cfg: SystemConfig,
    ) -> bool:
        """Process result from webhook or active call polling idempotently."""
        with self.transaction() as conn:
            existing_call = conn.execute(
                "SELECT * FROM calls WHERE request_id = ? OR call_sid = ?",
                (request_id, call_sid),
            ).fetchone()

            if existing_call and existing_call["state"] == "ended":
                # Duplicate delivery; already processed
                return False

            if existing_call:
                conn.execute(
                    """UPDATE calls 
                       SET call_sid = ?, state = 'ended', status = ?, disposition = ?,
                           callback_at = ?, started_at = ?, ended_at = ?, duration_sec = ?
                       WHERE request_id = ?""",
                    (
                        call_sid,
                        status,
                        disposition,
                        callback_at,
                        started_at,
                        ended_at,
                        duration_sec,
                        existing_call["request_id"],
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO calls 
                       (request_id, call_sid, tenant_id, campaign_id, lead_id, phone, attempt_no,
                        state, created_at, started_at, ended_at, status, disposition, callback_at, duration_sec)
                       VALUES (?, ?, ?, ?, ?, ?, 1, 'ended', ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        request_id,
                        call_sid,
                        tenant_id,
                        campaign_id,
                        lead_id,
                        phone,
                        started_at or ended_at or datetime.now(IST).isoformat(),
                        started_at,
                        ended_at,
                        status,
                        disposition,
                        callback_at,
                        duration_sec,
                    ),
                )

            lead = conn.execute(
                "SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?",
                (campaign_id, lead_id),
            ).fetchone()
            if not lead:
                return True

            camp = cfg.campaigns.get(campaign_id)
            max_attempts = camp.max_attempts if camp else 3

            # Determine attempt count: only increment if this call's acceptance was not already counted
            attempts = lead["attempts"]
            if not (lead["status"] == "active" and lead["last_call_sid"] == call_sid):
                attempts += 1

            # Determine next lead status following R7-R9
            next_eligible_at = None
            is_dnd = disposition == "dnd"

            if disposition in TERMINAL_DISPOSITIONS:
                new_status = "completed"
                if is_dnd:
                    conn.execute(
                        "INSERT OR IGNORE INTO dnd_registry (phone, marked_at, campaign_id, lead_id) VALUES (?, ?, ?, ?)",
                        (phone, ended_at or datetime.now(IST).isoformat(), campaign_id, lead_id),
                    )
                    # Global DND propagation to other non-terminal leads
                    conn.execute(
                        """UPDATE leads 
                           SET status = 'dnd_blocked' 
                           WHERE phone = ? AND status NOT IN ('completed', 'exhausted')""",
                        (phone,),
                    )
            elif attempts >= max_attempts:
                new_status = "exhausted"
            elif self._is_phone_in_dnd(phone, conn):
                new_status = "dnd_blocked"
            elif disposition == "callback_requested":
                new_status = "callback_scheduled"
                next_eligible_at = callback_at
            else:
                # no_answer, busy, or failed
                new_status = "retry_scheduled"
                wait_m = cfg.retry_policy.get_wait_minutes(status)
                if ended_at:
                    ended_dt = datetime.fromisoformat(ended_at)
                else:
                    ended_dt = datetime.now(IST)
                next_eligible_at = (ended_dt + timedelta(minutes=wait_m)).isoformat()

            conn.execute(
                """UPDATE leads 
                   SET status = ?, attempts = ?, last_call_sid = ?, last_status = ?,
                       last_disposition = ?, last_ended_at = ?, next_eligible_at = ?
                   WHERE campaign_id = ? AND lead_id = ?""",
                (
                    new_status,
                    attempts,
                    call_sid,
                    status,
                    disposition,
                    ended_at,
                    next_eligible_at,
                    campaign_id,
                    lead_id,
                ),
            )
            return True

    def _is_phone_in_dnd(self, phone: str, conn: sqlite3.Connection) -> bool:
        r = conn.execute("SELECT 1 FROM dnd_registry WHERE phone = ?", (phone,)).fetchone()
        return r is not None

    def get_calls_by_state(self, state: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute("SELECT * FROM calls WHERE state = ?", (state,)).fetchall()
            return [dict(r) for r in rows]

    def get_stuck_dialing_calls(self, min_age_seconds: float = 10.0) -> List[Dict[str, Any]]:
        cutoff = time.time() - min_age_seconds
        with self.get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM calls WHERE state = 'dialing' AND (real_created_at IS NULL OR real_created_at <= ?)",
                (cutoff,),
            ).fetchall()
            return [dict(r) for r in rows]

    def is_campaign_complete(self) -> bool:
        """True if all leads have reached terminal / final states and no active calls exist."""
        with self.get_connection() as conn:
            incomplete = conn.execute(
                "SELECT COUNT(*) as c FROM leads WHERE status NOT IN ('completed', 'exhausted', 'dnd_blocked')"
            ).fetchone()["c"]
            active_calls = conn.execute(
                "SELECT COUNT(*) as c FROM calls WHERE state IN ('dialing', 'active')"
            ).fetchone()["c"]
            return incomplete == 0 and active_calls == 0

    def get_final_states(self) -> List[Dict[str, Any]]:
        """Output records matching final_state.csv contract, sorted by campaign_id, lead_id."""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT campaign_id, lead_id, status AS final_state, attempts,
                       COALESCE(last_status, '') AS last_status,
                       COALESCE(last_disposition, '') AS last_disposition
                FROM leads
                ORDER BY campaign_id ASC, lead_id ASC
            """).fetchall()
            return [dict(r) for r in rows]

    def get_progress_summary(self) -> Dict[str, Any]:
        """Summary for monitoring and /status endpoint."""
        with self.get_connection() as conn:
            leads_by_status = dict(
                conn.execute("SELECT status, COUNT(*) FROM leads GROUP BY status").fetchall()
            )
            calls_by_state = dict(
                conn.execute("SELECT state, COUNT(*) FROM calls GROUP BY state").fetchall()
            )
            dnd_count = conn.execute("SELECT COUNT(*) FROM dnd_registry").fetchone()[0]
            return {
                "leads_by_status": leads_by_status,
                "calls_by_state": calls_by_state,
                "dnd_registered_phones": dnd_count,
            }
