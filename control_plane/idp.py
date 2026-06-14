"""Server-side client for the Agent IdP (the trusted-broker side).

The control plane holds the IdP's admin + internal API keys and brokers every
IdP call on behalf of agents — agents never see these keys. This wraps the
running ``Agent IdP Service`` (v0.2.0) endpoints:

  admin    : /agents (upsert/get), /grants, /grants/revoke, /audit/events,
             /skills (register/list/delete), /admin/approvals/*, /skill-grants/*
  internal : /attest/exchange, /capabilities/mint, /gateway/execute,
             /bc-authorize, /token, /introspect
  public   : /.well-known/jwks.json, /healthz

Configured via env: IDP_BASE_URL, IDP_ADMIN_API_KEY, IDP_INTERNAL_API_KEY.
If IDP_BASE_URL is unset the client is ``enabled == False`` and the control
plane simply skips identity provisioning (registration still works).
"""
from __future__ import annotations

import os
from typing import Any, Optional

import httpx


class IdPError(Exception):
    """Raised when the IdP returns a non-2xx response. Carries status + detail."""

    def __init__(self, status: int, detail: Any):
        self.status = status
        self.detail = detail
        super().__init__(f"idp_error {status}: {detail}")


class IdPClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        admin_api_key: Optional[str] = None,
        internal_api_key: Optional[str] = None,
        timeout: float = 15.0,
    ):
        self.base_url = (base_url or os.getenv("IDP_BASE_URL") or "").rstrip("/")
        self._admin = admin_api_key or os.getenv("IDP_ADMIN_API_KEY") or ""
        self._internal = internal_api_key or os.getenv("IDP_INTERNAL_API_KEY") or ""
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self._timeout)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def _admin_h(self) -> dict[str, str]:
        return {"X-Admin-API-Key": self._admin}

    @property
    def _internal_h(self) -> dict[str, str]:
        return {"X-Internal-API-Key": self._internal}

    async def _call(self, method: str, path: str, headers: dict, json: Any = None) -> Any:
        if not self.enabled:
            raise IdPError(503, "idp_not_configured")
        resp = await self._http().request(method, path, headers=headers, json=json)
        if resp.status_code >= 300:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise IdPError(resp.status_code, detail)
        if resp.headers.get("content-type", "").startswith("application/json"):
            return resp.json()
        return resp.text

    # -- health / keys -----------------------------------------------------
    async def healthz(self) -> Any:
        return await self._call("GET", "/healthz", {})

    async def jwks(self) -> Any:
        return await self._call("GET", "/.well-known/jwks.json", {})

    # -- agent identity (admin) -------------------------------------------
    async def upsert_agent(self, agent: dict[str, Any]) -> dict[str, Any]:
        return await self._call("POST", "/agents", self._admin_h, agent)

    async def get_agent(self, agent_id: str) -> dict[str, Any]:
        return await self._call("GET", f"/agents/{agent_id}", self._admin_h)

    # -- attestation -> agent access token (internal) ---------------------
    async def attest_exchange(self, attestation: dict[str, Any]) -> dict[str, Any]:
        return await self._call("POST", "/attest/exchange", self._internal_h, attestation)

    # -- grants (admin) ----------------------------------------------------
    async def create_grant(self, grant: dict[str, Any]) -> dict[str, Any]:
        return await self._call("POST", "/grants", self._admin_h, grant)

    async def revoke(self, grant_id: Optional[str] = None, jti: Optional[str] = None) -> Any:
        return await self._call("POST", "/grants/revoke", self._admin_h,
                                {"grant_id": grant_id, "jti": jti})

    # -- capabilities + gateway (internal) --------------------------------
    async def mint_capability(self, req: dict[str, Any]) -> dict[str, Any]:
        return await self._call("POST", "/capabilities/mint", self._internal_h, req)

    async def gateway_execute(self, req: dict[str, Any]) -> dict[str, Any]:
        return await self._call("POST", "/gateway/execute", self._internal_h, req)

    async def introspect(self, token: str) -> dict[str, Any]:
        return await self._call("POST", "/introspect", self._internal_h, {"token": token})

    # -- skills (admin) + CIBA (internal) ---------------------------------
    async def register_skill(self, skill: dict[str, Any]) -> dict[str, Any]:
        return await self._call("POST", "/skills", self._admin_h, skill)

    async def list_skills(self) -> dict[str, Any]:
        return await self._call("GET", "/skills", self._admin_h)

    async def delete_skill(self, skill_id: str) -> Any:
        return await self._call("DELETE", f"/skills/{skill_id}", self._admin_h)

    async def bc_authorize(self, req: dict[str, Any]) -> dict[str, Any]:
        return await self._call("POST", "/bc-authorize", self._internal_h, req)

    async def ciba_token(self, auth_req_id: str) -> tuple[int, dict[str, Any]]:
        """Returns (status_code, body). 400 + {error:authorization_pending|slow_down}
        is normal polling, so callers branch on the body rather than raising."""
        if not self.enabled:
            raise IdPError(503, "idp_not_configured")
        resp = await self._http().post(
            "/token", headers=self._internal_h,
            json={"grant_type": "urn:openid:params:grant-type:ciba", "auth_req_id": auth_req_id},
        )
        try:
            body = resp.json()
        except Exception:
            body = {"error": "non_json", "raw": resp.text}
        return resp.status_code, body

    async def approve(self, auth_req_id: str, decided_by: str, reason: Optional[str] = None) -> Any:
        return await self._call("POST", f"/admin/approvals/{auth_req_id}/approve",
                                self._admin_h, {"decided_by": decided_by, "reason": reason})

    async def deny(self, auth_req_id: str, decided_by: str, reason: Optional[str] = None) -> Any:
        return await self._call("POST", f"/admin/approvals/{auth_req_id}/deny",
                                self._admin_h, {"decided_by": decided_by, "reason": reason})

    async def pending_approvals(self) -> dict[str, Any]:
        return await self._call("GET", "/admin/pending-approvals", self._admin_h)

    async def revoke_skill_grant(self, grant_id: str) -> Any:
        return await self._call("POST", f"/skill-grants/{grant_id}/revoke", self._admin_h)

    # -- audit (admin) -----------------------------------------------------
    async def audit_events(self, limit: int = 100) -> dict[str, Any]:
        return await self._call("GET", f"/audit/events?limit={limit}", self._admin_h)
