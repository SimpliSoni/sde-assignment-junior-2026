"""Asynchronous HTTP client for communicating with the mock telephony provider."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("telephony_client")


class TelephonyClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8099", timeout: float = 8.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=3.0),
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get_clock(self) -> Dict[str, Any]:
        """Fetch current clock info from provider."""
        client = await self.get_client()
        resp = await client.get("/v1/clock")
        resp.raise_for_status()
        return resp.json()

    async def place_call(
        self,
        request_id: str,
        campaign_id: str,
        lead_id: str,
        phone: str,
        callback_url: str,
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Place call to provider.
        Returns (status_code, body).
        On 504, timeout, or connection error, returns (504, {"error": "upstream_timeout"}).
        """
        client = await self.get_client()
        payload = {
            "request_id": request_id,
            "campaign_id": campaign_id,
            "lead_id": lead_id,
            "phone": phone,
            "callback_url": callback_url,
        }
        try:
            resp = await client.post("/v1/calls", json=payload)
            try:
                data = resp.json()
            except Exception:
                data = {"raw_text": resp.text}
            return resp.status_code, data
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            logger.warning("Network timeout or error placing call %s: %s", request_id, e)
            return 504, {"error": "upstream_timeout", "detail": str(e)}

    async def get_call_by_request_id(self, request_id: str) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Query call state by request_id. Returns (status_code, data)."""
        client = await self.get_client()
        try:
            resp = await client.get("/v1/calls", params={"request_id": request_id})
            if resp.status_code == 200:
                return 200, resp.json()
            return resp.status_code, None
        except Exception as e:
            logger.error("Failed to query call by request_id %s: %s", request_id, e)
            return 500, None

    async def get_call_by_sid(self, call_sid: str) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Query call state by call_sid. Returns (status_code, data)."""
        client = await self.get_client()
        try:
            resp = await client.get(f"/v1/calls/{call_sid}")
            if resp.status_code == 200:
                return 200, resp.json()
            return resp.status_code, None
        except Exception as e:
            logger.error("Failed to query call by call_sid %s: %s", call_sid, e)
            return 500, None

    async def get_admin_report(self) -> Dict[str, Any]:
        """Fetch grading report from provider."""
        client = await self.get_client()
        resp = await client.get("/admin/report")
        resp.raise_for_status()
        return resp.json()

    async def get_admin_leads(self) -> List[Dict[str, Any]]:
        """Fetch expected lead states from provider."""
        client = await self.get_client()
        resp = await client.get("/admin/leads")
        resp.raise_for_status()
        return resp.json()

