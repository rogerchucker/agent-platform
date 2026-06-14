"""Agent SDK for the SRE control plane — one client for every platform feature.

A single ``ControlPlaneClient`` covers:
  • registration (which provisions an IdP identity under the hood),
  • liveness (auto-heartbeat) and status,
  • the message queue (listen + publish, REST or WebSocket),
  • identity & authorization brokered through the control plane to the IdP:
    attestation → access token → grant → capability → gateway execution,
    plus skill access (CIBA) and token introspection.

Agents talk ONLY to the control plane; the admin/internal IdP keys never leave
the server. The control plane brokers each identity call.

Example
-------
    async with ControlPlaneClient("http://sre-control-plane") as cp:
        await cp.register(
            name="rootcause-db", kind="rootcause",
            capabilities=["db-rootcause"], subscriptions=["tasks.rootcause"],
            identity={                       # ← provisions an IdP identity
                "owner_principal": "you@example.com",
                "trust_level": "low", "allowed_envs": ["dev"],
                "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}],
            },
        )
        # mint a scoped capability and act through the gateway
        token = await cp.get_access_token(runtime={"kind": "cloud", "cluster": "local-dev"})
        grant = await cp.request_grant(action="k8s.get", resource="pods/checkout",
                                       purpose="rootcause", reason="crashloop", ticket="INC-42")
        cap = await cp.mint_capability(token, grant["grant_id"], "k8s.get", "pods/checkout",
                                       purpose="rootcause", reason="crashloop", ticket="INC-42")
        await cp.execute(cap["capability_token"], tool="kubernetes",
                         action="k8s.get", resource="pods/checkout")

Requires ``httpx`` and ``websockets``.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Optional

import httpx


class ControlPlaneClient:
    def __init__(self, base_url: str = "http://localhost:8080", heartbeat_interval: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.heartbeat_interval = heartbeat_interval
        self.agent_id: Optional[str] = None
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=20.0)
        self._hb_task: Optional[asyncio.Task] = None

    async def __aenter__(self) -> "ControlPlaneClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def close(self) -> None:
        if self._hb_task:
            self._hb_task.cancel()
        await self._http.aclose()

    async def _post(self, path: str, json_body: Any) -> Any:
        resp = await self._http.post(path, json=json_body)
        resp.raise_for_status()
        return resp.json()

    # -- registration / status (+ IdP provisioning) ----------------------
    async def register(
        self,
        name: str,
        kind: str = "sre",
        capabilities: Optional[list[str]] = None,
        subscriptions: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        agent_id: Optional[str] = None,
        identity: Optional[dict[str, Any]] = None,
        auto_heartbeat: bool = True,
    ) -> dict:
        """Register with the control plane. If ``identity`` is provided, the
        control plane also provisions a matching IdP identity (same agent_id).
        The returned agent has ``idp_provisioned`` / ``idp_error`` set."""
        resp = await self._http.post("/agents", json={
            "name": name, "kind": kind,
            "capabilities": capabilities or [],
            "subscriptions": subscriptions or [],
            "metadata": metadata or {},
            "agent_id": agent_id,
            "identity": identity,
        })
        resp.raise_for_status()
        agent = resp.json()
        self.agent_id = agent["agent_id"]
        if auto_heartbeat:
            self._hb_task = asyncio.create_task(self._heartbeat_loop())
        return agent

    async def heartbeat(self) -> dict:
        resp = await self._http.post(f"/agents/{self.agent_id}/heartbeat")
        resp.raise_for_status()
        return resp.json()

    async def deregister(self) -> None:
        if self.agent_id:
            await self._http.delete(f"/agents/{self.agent_id}")

    async def list_agents(self, include_deregistered: bool = False) -> list[dict]:
        """All agents registered on the platform (used to find peers)."""
        resp = await self._http.get("/agents", params={"include_deregistered": include_deregistered})
        resp.raise_for_status()
        return resp.json()["agents"]

    async def get_identity(self, agent_id: Optional[str] = None) -> dict:
        """The IdP record provisioned for an agent (defaults to self)."""
        resp = await self._http.get(f"/agents/{agent_id or self.agent_id}/identity")
        resp.raise_for_status()
        return resp.json()

    async def clone_agent(
        self,
        source_agent_id: str,
        new_agent_id: Optional[str] = None,
        name: Optional[str] = None,
        clone_bindings: bool = False,
        owner_principal: Optional[str] = None,
    ) -> dict:
        """Fork an agent into a new one (fresh identity). clone_bindings=True makes
        a replica that shares the source's workload identity (scale-out); the
        default is an independent clone that must bind its own runtime."""
        body: dict[str, Any] = {"clone_bindings": clone_bindings}
        if new_agent_id is not None:
            body["new_agent_id"] = new_agent_id
        if name is not None:
            body["name"] = name
        if owner_principal is not None:
            body["owner_principal"] = owner_principal
        resp = await self._http.post(f"/agents/{source_agent_id}/clone", json=body)
        resp.raise_for_status()
        return resp.json()

    async def delete_agent(self, agent_id: str, hard: bool = False) -> dict:
        """Decommission an agent: deregisters locally and tears down the IdP
        identity (revoke grants/tokens/skill-grants + disable, or hard-delete)."""
        resp = await self._http.request(
            "DELETE", f"/agents/{agent_id}", params={"hard": str(hard).lower()}
        )
        resp.raise_for_status()
        return resp.json()

    async def try_attest(self, agent_id: str, runtime: dict[str, Any], env: str = "dev") -> tuple[int, dict]:
        """Attempt an attestation/token exchange for an arbitrary agent id WITHOUT
        raising — returns (status, body). Used to prove a decommissioned agent can
        no longer act (expect 403 agent_disabled)."""
        resp = await self._http.post(
            f"/agents/{agent_id}/identity/token",
            json={"env": env, "runtime": runtime, "session_id": "probe", "trace_id": "probe"},
        )
        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, {}

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                await self.heartbeat()
            except Exception:
                pass

    # -- message queue (REST) --------------------------------------------
    async def publish(self, topic: str, payload: dict[str, Any]) -> dict:
        return await self._post("/messages", {
            "topic": topic, "payload": payload, "sender": self.agent_id,
        })

    # -- message queue (WebSocket stream) --------------------------------
    async def listen(self, topics: list[str]) -> AsyncIterator[dict]:
        """Yield incoming messages over a WebSocket. Heartbeats handled inline."""
        import websockets

        ws_url = self.base_url.replace("http", "ws", 1) + f"/ws/{self.agent_id}"
        ws_url += "?topics=" + ",".join(topics)
        async with websockets.connect(ws_url) as ws:
            async def beat():
                while True:
                    await asyncio.sleep(self.heartbeat_interval)
                    await ws.send(json.dumps({"type": "heartbeat"}))

            beat_task = asyncio.create_task(beat())
            try:
                async for raw in ws:
                    frame = json.loads(raw)
                    if frame.get("type") == "message":
                        yield frame
            finally:
                beat_task.cancel()

    # -- identity & authorization (brokered to the IdP) ------------------
    async def get_access_token(
        self,
        runtime: dict[str, Any],
        env: str = "dev",
        session_id: str = "sess",
        trace_id: str = "trace",
    ) -> str:
        """Exchange a runtime attestation for an agent access token."""
        out = await self._post(f"/agents/{self.agent_id}/identity/token", {
            "env": env, "runtime": runtime, "session_id": session_id, "trace_id": trace_id,
        })
        return out["access_token"]

    async def request_grant(
        self,
        action: str,
        resource: str,
        purpose: str,
        reason: str,
        ticket: str,
        env: str = "dev",
        grant_type: str = "policy_auto",
        granted_by: str = "control-plane",
        mfa: bool = False,
        ttl_seconds: int = 1800,
    ) -> dict:
        return await self._post(f"/agents/{self.agent_id}/identity/grants", {
            "action": action, "resource": resource, "purpose": purpose, "reason": reason,
            "ticket": ticket, "env": env, "grant_type": grant_type, "granted_by": granted_by,
            "mfa": mfa, "ttl_seconds": ttl_seconds,
        })

    async def mint_capability(
        self,
        agent_access_token: str,
        grant_id: str,
        cap_action: str,
        cap_resource: str,
        purpose: str,
        reason: str,
        ticket: str,
        session_id: str = "sess",
        trace_id: str = "trace",
        risk_level: str = "low",
        constraints: Optional[dict[str, Any]] = None,
        limits: Optional[dict[str, Any]] = None,
    ) -> dict:
        return await self._post("/idp/capabilities/mint", {
            "agent_access_token": agent_access_token, "grant_id": grant_id,
            "cap_action": cap_action, "cap_resource": cap_resource,
            "purpose": purpose, "reason": reason, "ticket": ticket,
            "session_id": session_id, "trace_id": trace_id, "risk_level": risk_level,
            "constraints": constraints or {}, "limits": limits or {},
        })

    async def execute(
        self,
        capability_token: str,
        tool: str,
        action: str,
        resource: str,
        params: Optional[dict[str, Any]] = None,
        presenter: Optional[str] = None,
    ) -> dict:
        return await self._post("/idp/execute", {
            "capability_token": capability_token, "tool": tool, "action": action,
            "resource": resource, "params": params or {}, "presenter": presenter,
        })

    async def introspect(self, token: str) -> dict:
        return await self._post("/idp/introspect", {"token": token})

    # -- incy bridge (trigger + read proxies) ----------------------------
    async def incy_trigger(self, summary: str, severity: str = "critical",
                           dedup_key: Optional[str] = None) -> dict:
        params: dict[str, Any] = {"summary": summary, "severity": severity}
        if dedup_key:
            params["dedup_key"] = dedup_key
        resp = await self._http.post("/incy/trigger", params=params)
        resp.raise_for_status()
        return resp.json()

    async def incy_incidents(self, status: Optional[str] = None, limit: int = 20) -> list[dict]:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        resp = await self._http.get("/incy/incidents", params=params)
        resp.raise_for_status()
        return resp.json()["incidents"]

    async def incy_incident(self, incident_id: str) -> dict:
        resp = await self._http.get(f"/incy/incidents/{incident_id}")
        resp.raise_for_status()
        return resp.json()

    # -- skill access (CIBA) ---------------------------------------------
    async def authorize_skill(
        self,
        skill_id: str,
        action: str = "read",
        login_hint: Optional[str] = None,
        binding_message: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> dict:
        """Start back-channel authorization for a skill. Returns auth_req_id."""
        return await self._post("/idp/skills/authorize", {
            "skill_id": skill_id, "action": action,
            "login_hint": login_hint or f"agent:{self.agent_id}",
            "binding_message": binding_message, "reason": reason,
        })

    async def poll_skill_token(self, auth_req_id: str) -> tuple[int, dict]:
        """One CIBA poll. Returns (status, body); body.error is
        authorization_pending / slow_down while awaiting human approval."""
        resp = await self._http.post("/idp/skills/token", params={"auth_req_id": auth_req_id})
        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, {"error": "non_json"}

    async def approve_skill(
        self, auth_req_id: str, reason: Optional[str] = None
    ) -> dict:
        """Approve a pending skill (CIBA) request as this agent (the approver)."""
        return await self._post("/idp/skills/approve", {
            "auth_req_id": auth_req_id, "approver_agent_id": self.agent_id,
            "decision": "approve", "reason": reason,
        })

    async def deny_skill(self, auth_req_id: str, reason: Optional[str] = None) -> dict:
        return await self._post("/idp/skills/approve", {
            "auth_req_id": auth_req_id, "approver_agent_id": self.agent_id,
            "decision": "deny", "reason": reason,
        })

    async def mint_skill_token(self, auth_req_id: str, attempts: int = 3, interval: float = 1.0) -> dict:
        """Poll a (now-approved) CIBA request and return the minted token body."""
        for _ in range(attempts):
            status, body = await self.poll_skill_token(auth_req_id)
            if status == 200:
                return body
            if body.get("error") in ("authorization_pending", "slow_down"):
                await asyncio.sleep(interval)
                continue
            raise PermissionError(f"skill access {body.get('error')}")
        raise TimeoutError("token not ready after approval")

    async def request_skill_access(
        self,
        skill_id: str,
        action: str = "read",
        timeout: float = 60.0,
        **kw,
    ) -> dict:
        """Convenience: authorize a skill and poll until approved/denied/timeout.
        Returns the token body on success; raises TimeoutError/PermissionError."""
        bc = await self.authorize_skill(skill_id, action=action, **kw)
        auth_req_id = bc["auth_req_id"]
        interval = bc.get("interval", 5)
        waited = 0.0
        while waited <= timeout:
            status, body = await self.poll_skill_token(auth_req_id)
            if status == 200:
                return body
            err = body.get("error")
            if err in ("authorization_pending", "slow_down"):
                await asyncio.sleep(interval)
                waited += interval
                continue
            raise PermissionError(f"skill access {err}")
        raise TimeoutError("skill authorization timed out")
