"""Incy connector — bridges the incy incident system to the message queue.

The control plane runs in-cluster, so it can reach ``incy-api`` directly and it
owns the message queue in-process. This connector wires the two together on a
single shared topic (default ``incidents``):

  incy → queue : a poller watches incy for newly *triggered* incidents and
                 publishes each onto the topic (status ``unacked``).
  queue → incy : a consumer reads agent status updates off the topic and applies
                 them to incy so its UI reflects them:
                   status "investigating" → POST /acknowledge  (+ note: picked up)
                   status "closed"        → POST /resolve       (+ note)

Three incident states are surfaced, mapping to incy's native lifecycle:
  unacked → triggered,  investigating → acknowledged,  closed → resolved.

Configured via env: INCY_BASE_URL, INCY_INTEGRATION_KEY (to create demo events),
INCY_AGENT_USER_ID (the incy user recorded as ack/resolve actor — i.e. "the
agent"). Disabled (no-op) when INCY_BASE_URL is unset.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import httpx

from .message_queue import MessageQueue
from .models import Message

logger = logging.getLogger("incy_connector")

INCIDENTS_TOPIC = os.getenv("INCIDENTS_TOPIC", "incidents")
# How agent status values map onto incy actions.
_STATUS_ACTIONS = {"investigating": "acknowledge", "closed": "resolve"}


class IncyConnector:
    def __init__(
        self,
        mq: MessageQueue,
        base_url: Optional[str] = None,
        integration_key: Optional[str] = None,
        agent_user_id: Optional[str] = None,
        poll_interval: float = 5.0,
    ):
        self.mq = mq
        self.base_url = (base_url or os.getenv("INCY_BASE_URL") or "").rstrip("/")
        self.integration_key = integration_key or os.getenv("INCY_INTEGRATION_KEY") or ""
        self.agent_user_id = agent_user_id or os.getenv("INCY_AGENT_USER_ID") or ""
        self.poll_interval = poll_interval
        self._client: Optional[httpx.AsyncClient] = None
        self._tasks: list[asyncio.Task] = []
        self._seen: set[str] = set()

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        return self._client

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        if not self.enabled:
            logger.info("incy connector disabled (no INCY_BASE_URL)")
            return
        # Seed seen-set with already-open incidents so we only surface NEW ones.
        try:
            for inc in await self._list_triggered():
                self._seen.add(inc["id"])
        except Exception as exc:  # pragma: no cover - network
            logger.warning("incy seed failed: %s", exc)
        self._tasks = [
            asyncio.create_task(self._poll_loop()),
            asyncio.create_task(self._consume_loop()),
        ]
        logger.info("incy connector started (base_url=%s)", self.base_url)

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks = []
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -- incy → queue ------------------------------------------------------
    async def _list_triggered(self) -> list[dict]:
        resp = await self._http().get("/v1/incidents", params={"status": "triggered", "limit": 50})
        resp.raise_for_status()
        return resp.json().get("incidents", [])

    async def _poll_loop(self) -> None:
        while True:
            try:
                for inc in await self._list_triggered():
                    if inc["id"] in self._seen:
                        continue
                    self._seen.add(inc["id"])
                    await self.mq.publish(Message(
                        topic=INCIDENTS_TOPIC, sender="incy",
                        payload={
                            "kind": "incident",
                            "incident_id": inc["id"],
                            "incident_number": inc.get("incident_number"),
                            "title": inc.get("title"),
                            "severity": inc.get("severity"),
                            "service_id": inc.get("service_id"),
                            "status": "unacked",
                            "source": "incy",
                        },
                    ))
                    logger.info("published incy incident %s", inc["id"])
            except Exception as exc:  # pragma: no cover - network
                logger.warning("incy poll error: %s", exc)
            await asyncio.sleep(self.poll_interval)

    # -- queue → incy ------------------------------------------------------
    async def _consume_loop(self) -> None:
        sub = await self.mq.subscribe("incy-connector", [INCIDENTS_TOPIC])
        while True:
            msg = await sub.queue.get()
            payload = msg.payload or {}
            if payload.get("kind") != "status":
                continue  # ignore incident announcements (incl. our own)
            await self._apply_status(payload, msg.sender)

    async def _apply_status(self, payload: dict, sender: str) -> None:
        incident_id = payload.get("incident_id")
        status = payload.get("status")
        if not incident_id:
            return
        headers = {"X-User-Id": self.agent_user_id} if self.agent_user_id else {}
        agent = payload.get("agent_name") or sender

        # "escalated" = the agent could NOT remediate (denied / failed) and is
        # handing the incident back to a human: add a note ONLY, leaving the
        # incident OPEN. It must not resolve.
        if status == "escalated":
            note = payload.get("note") or f"Escalated by {agent} — needs human attention"
            try:
                await self._http().post(
                    f"/v1/incidents/{incident_id}/notes", headers=headers, json={"content": note})
                logger.info("incy incident %s escalated (note only) by %s", incident_id, agent)
            except Exception as exc:  # pragma: no cover - network
                logger.warning("incy note failed for %s: %s", incident_id, exc)
            return

        action = _STATUS_ACTIONS.get(status)
        if not action:
            return
        try:
            r = await self._http().post(f"/v1/incidents/{incident_id}/{action}", headers=headers)
            r.raise_for_status()
            note = payload.get("note") or (
                f"Picked up by {agent} — investigating" if status == "investigating"
                else f"Resolved by {agent}")
            await self._http().post(
                f"/v1/incidents/{incident_id}/notes", headers=headers, json={"content": note})
            logger.info("incy incident %s -> %s by %s", incident_id, action, agent)
        except Exception as exc:  # pragma: no cover - network
            logger.warning("incy %s failed for %s: %s", action, incident_id, exc)

    # -- read proxies (so the dashboard/demos can show incy state) ---------
    async def get_incident(self, incident_id: str) -> dict:
        resp = await self._http().get(f"/v1/incidents/{incident_id}")
        resp.raise_for_status()
        return resp.json()

    async def list_incidents(self, status: Optional[str] = None, limit: int = 20) -> list[dict]:
        params: dict = {"limit": limit}
        if status:
            params["status"] = status
        resp = await self._http().get("/v1/incidents", params=params)
        resp.raise_for_status()
        return resp.json().get("incidents", [])

    # -- demo helper -------------------------------------------------------
    async def trigger_event(self, summary: str, severity: str = "warning",
                            dedup_key: Optional[str] = None) -> dict:
        """Create a real incy incident via the Events API (for demos)."""
        body = {"integration_key": self.integration_key, "summary": summary, "severity": severity,
                "source": "sre-platform"}
        if dedup_key:
            body["dedup_key"] = dedup_key
        resp = await self._http().post("/v1/events", json=body)
        resp.raise_for_status()
        return resp.json()
