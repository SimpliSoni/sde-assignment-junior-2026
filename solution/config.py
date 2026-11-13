"""Configuration models, loaders, and time/window utilities for the Campaign Dispatcher."""
from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}


def parse_hm(s: str) -> Tuple[int, int]:
    parts = s.split(":")
    return int(parts[0]), int(parts[1])


def in_window(
    t: datetime,
    start: str,
    end: str,
    days: Optional[List[str]] = None,
    grace_s: float = 0.0,
    buffer_before_end_s: float = 0.0,
) -> bool:
    """Check if simulated time t is within [start, end - buffer) on an allowed day, with optional grace seconds."""
    for probe in (t, t - timedelta(seconds=grace_s), t + timedelta(seconds=grace_s)):
        lt = probe.astimezone(IST)
        if days is not None and DAYS[lt.weekday()] not in days:
            continue
        sh, sm = parse_hm(start)
        eh, em = parse_hm(end)
        s = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        e = lt.replace(hour=eh, minute=em, second=0, microsecond=0) - timedelta(seconds=buffer_before_end_s)
        if s <= lt < e:
            return True
    return False


def next_open(
    t: datetime,
    start: str,
    end: str,
    days: List[str],
) -> datetime:
    """Earliest instant >= t (in IST) that falls inside [start, end) on an allowed day."""
    lt = t.astimezone(IST)
    sh, sm = parse_hm(start)
    eh, em = parse_hm(end)

    for d in range(0, 9):
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        if DAYS[day.weekday()] not in days:
            continue
        s = day.replace(hour=sh, minute=sm)
        e = day.replace(hour=eh, minute=em)
        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s
    return lt


def is_in_calling_window(
    t: datetime,
    campaign_start: str,
    campaign_end: str,
    campaign_days: List[str],
    regulatory_start: str = "09:00",
    regulatory_end: str = "21:00",
    grace_s: float = 0.0,
    buffer_before_end_s: float = 180.0,
) -> bool:
    """Check if t is within BOTH the campaign window and the regulatory window (09:00-21:00 IST) with buffer before close."""
    # Regulatory check (all days)
    if not in_window(t, regulatory_start, regulatory_end, None, grace_s=grace_s, buffer_before_end_s=buffer_before_end_s):
        return False
    # Campaign check
    return in_window(t, campaign_start, campaign_end, campaign_days, grace_s=grace_s, buffer_before_end_s=buffer_before_end_s)


def next_eligible_window_open(
    t: datetime,
    campaign_start: str,
    campaign_end: str,
    campaign_days: List[str],
    regulatory_start: str = "09:00",
    regulatory_end: str = "21:00",
) -> datetime:
    """Find next open instant respecting both campaign window and regulatory bounds."""
    # Effective window bounds
    c_sh, c_sm = parse_hm(campaign_start)
    r_sh, r_sm = parse_hm(regulatory_start)
    eff_sh, eff_sm = max((c_sh, c_sm), (r_sh, r_sm))
    eff_start = f"{eff_sh:02d}:{eff_sm:02d}"

    c_eh, c_em = parse_hm(campaign_end)
    r_eh, r_em = parse_hm(regulatory_end)
    eff_eh, eff_em = min((c_eh, c_em), (r_eh, r_em))
    eff_end = f"{eff_eh:02d}:{eff_em:02d}"

    return next_open(t, eff_start, eff_end, campaign_days)


class SimClock:
    """Simulated clock tracking the provider's clock without drift."""

    def __init__(self, sim_epoch: datetime, real_epoch: float, speed: float):
        self.sim_epoch = sim_epoch.astimezone(IST)
        self.real_epoch = real_epoch
        self.speed = speed

    def now(self) -> datetime:
        elapsed_real = time.time() - self.real_epoch
        return self.sim_epoch + timedelta(seconds=elapsed_real * self.speed)

    def sim_to_real_seconds(self, sim_seconds: float) -> float:
        if self.speed <= 0:
            return sim_seconds
        return sim_seconds / self.speed

    def real_to_sim_seconds(self, real_seconds: float) -> float:
        return real_seconds * self.speed

    def update_from_provider(self, sim_epoch: datetime, real_epoch: float, speed: float):
        self.sim_epoch = sim_epoch.astimezone(IST)
        self.real_epoch = real_epoch
        self.speed = speed


@dataclass
class TenantConfig:
    tenant_id: str
    max_concurrent_calls: int


@dataclass
class CampaignConfig:
    campaign_id: str
    tenant_id: str
    leads_file: str
    window_start: str
    window_end: str
    window_days: List[str]
    max_attempts: int


@dataclass
class RetryPolicy:
    no_answer_minutes: int = 60
    busy_minutes: int = 60
    failed_minutes: int = 15

    def get_wait_minutes(self, status: str) -> int:
        if status == "no_answer":
            return self.no_answer_minutes
        elif status == "busy":
            return self.busy_minutes
        elif status == "failed":
            return self.failed_minutes
        return self.no_answer_minutes


@dataclass
class SystemConfig:
    sim_start: datetime
    timezone_name: str
    regulatory_start: str
    regulatory_end: str
    window_grace_seconds: int
    retry_policy: RetryPolicy
    webhook_secret: str
    tenants: Dict[str, TenantConfig] = field(default_factory=dict)
    campaigns: Dict[str, CampaignConfig] = field(default_factory=dict)
    fixtures_dir: str = ""

    @classmethod
    def load_from_file(cls, fixtures_path: str) -> "SystemConfig":
        with open(fixtures_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        fixtures_dir = os.path.dirname(os.path.abspath(fixtures_path))
        sim_start = datetime.fromisoformat(raw["sim_start"])
        if sim_start.tzinfo is None:
            sim_start = sim_start.replace(tzinfo=IST)

        retry_raw = raw.get("retry_policy", {})
        retry_policy = RetryPolicy(
            no_answer_minutes=retry_raw.get("no_answer_minutes", 60),
            busy_minutes=retry_raw.get("busy_minutes", 60),
            failed_minutes=retry_raw.get("failed_minutes", 15),
        )

        cfg = cls(
            sim_start=sim_start,
            timezone_name=raw.get("timezone", "Asia/Kolkata"),
            regulatory_start=raw["regulatory_window"]["start"],
            regulatory_end=raw["regulatory_window"]["end"],
            window_grace_seconds=raw.get("window_grace_seconds", 120),
            retry_policy=retry_policy,
            webhook_secret=raw.get("webhook_secret", ""),
            fixtures_dir=fixtures_dir,
        )

        for t in raw.get("tenants", []):
            cfg.tenants[t["tenant_id"]] = TenantConfig(
                tenant_id=t["tenant_id"],
                max_concurrent_calls=t["max_concurrent_calls"],
            )

        for c in raw.get("campaigns", []):
            w = c["window"]
            cfg.campaigns[c["campaign_id"]] = CampaignConfig(
                campaign_id=c["campaign_id"],
                tenant_id=c["tenant_id"],
                leads_file=c["leads_file"],
                window_start=w["start"],
                window_end=w["end"],
                window_days=[d.lower() for d in w["days"]],
                max_attempts=c.get("max_attempts", 3),
            )

        return cfg

    def load_leads(self, campaign_id: str) -> List[Dict[str, str]]:
        camp = self.campaigns.get(campaign_id)
        if not camp:
            raise ValueError(f"Unknown campaign: {campaign_id}")
        path = os.path.join(self.fixtures_dir, camp.leads_file)
        leads = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                leads.append({
                    "campaign_id": campaign_id,
                    "tenant_id": camp.tenant_id,
                    "lead_id": row["lead_id"],
                    "phone": row["phone"],
                    "customer_name": row.get("customer_name", ""),
                })
        return leads
