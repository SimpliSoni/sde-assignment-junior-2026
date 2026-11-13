"""FastAPI webhook receiver and status monitor."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from typing import Any, Callable, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from .config import SystemConfig
from .db import Database

logger = logging.getLogger("webhook_server")


def create_webhook_app(
    db: Database,
    cfg: SystemConfig,
    wake_event: Optional[asyncio.Event] = None,
    clock_fn: Optional[Callable[[], Any]] = None,
) -> FastAPI:
    app = FastAPI(title="Agentic Universe - Webhook Receiver", version="1.0")

    @app.post("/webhooks/telephony")
    async def telephony_webhook(
        request: Request,
        x_au_signature: Optional[str] = Header(None, alias="X-AU-Signature"),
    ):
        raw_body = await request.body()

        # 1. HMAC Signature verification
        if not cfg.webhook_secret:
            logger.warning("No webhook_secret configured; skipping signature verification")
        else:
            if not x_au_signature:
                logger.warning("Missing X-AU-Signature header")
                raise HTTPException(status_code=401, detail="missing_signature")

            expected_sig = hmac.new(
                cfg.webhook_secret.encode("utf-8"),
                raw_body,
                hashlib.sha256,
            ).hexdigest()

            if not hmac.compare_digest(x_au_signature, expected_sig):
                logger.warning("Invalid X-AU-Signature received: %s vs %s", x_au_signature, expected_sig)
                raise HTTPException(status_code=401, detail="invalid_signature")

        # 2. Parse payload
        try:
            payload = await request.json()
        except Exception as e:
            logger.error("Failed to parse JSON body: %s", e)
            raise HTTPException(status_code=400, detail="invalid_json")

        call_sid = payload.get("call_sid")
        request_id = payload.get("request_id")
        if not call_sid or not request_id:
            raise HTTPException(status_code=400, detail="missing_call_sid_or_request_id")

        # 3. Process result idempotently
        processed = db.process_call_result(
            call_sid=call_sid,
            request_id=request_id,
            tenant_id=payload.get("tenant_id", ""),
            campaign_id=payload.get("campaign_id", ""),
            lead_id=payload.get("lead_id", ""),
            phone=payload.get("phone", ""),
            status=payload.get("status", ""),
            disposition=payload.get("disposition"),
            callback_at=payload.get("callback_at"),
            started_at=payload.get("started_at"),
            ended_at=payload.get("ended_at"),
            duration_sec=payload.get("duration_sec", 0),
            cfg=cfg,
        )

        # 4. Notify dispatch loop of freed capacity / state change
        if wake_event is not None:
            wake_event.set()

        return JSONResponse({"status": "ok", "newly_processed": processed}, status_code=200)

    @app.get("/status")
    async def get_status():
        prog = db.get_progress_summary()
        sim_now_str = None
        if clock_fn:
            sim_now_str = clock_fn().isoformat()
        return {
            "sim_now": sim_now_str,
            "progress": prog,
            "completed": db.is_campaign_complete(),
        }

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app

