"""Tests for the control plane's IdP broker.

Uses a fake IdP client swapped into the app so we exercise the broker wiring
(registration → provisioning, token/grant/capability/execute, skill CIBA)
without a live IdP.
"""
import pytest
from fastapi.testclient import TestClient

import control_plane.main as cpmain


class FakeIdP:
    enabled = True
    base_url = "http://fake-idp"

    def __init__(self):
        self.calls = []

    async def upsert_agent(self, agent):
        self.calls.append(("upsert_agent", agent))
        return {**agent, "status": "active"}

    async def get_agent(self, agent_id):
        return {"agent_id": agent_id, "status": "active", "tenant": "org:democorp"}

    async def attest_exchange(self, att):
        self.calls.append(("attest", att))
        return {"access_token": "access-xyz", "token_type": "bearer", "expires_in": 900}

    async def create_grant(self, grant):
        self.calls.append(("grant", grant))
        return {"grant_id": "grant-1", "status": "approved", **grant}

    async def mint_capability(self, req):
        self.calls.append(("mint", req))
        return {"capability_token": "cap-xyz", "expires_in": 300, "jti": "jti-1"}

    async def gateway_execute(self, req):
        self.calls.append(("execute", req))
        return {"status": "executed", "tool": req["tool"], "action": req["action"]}

    async def introspect(self, token):
        return {"active": True, "token": token}

    async def bc_authorize(self, body):
        self.calls.append(("bc", body))
        return {"auth_req_id": "ciba-1", "expires_in": 300, "interval": 5}

    async def ciba_token(self, auth_req_id):
        return 200, {"access_token": "skill-tok", "scope": "skill:s:read", "grant_id": "sg-1"}

    async def approve(self, auth_req_id, decided_by, reason=None):
        self.calls.append(("approve", auth_req_id, decided_by))
        return {"auth_req_id": auth_req_id, "status": "approved", "decided_by": decided_by}

    async def deny(self, auth_req_id, decided_by, reason=None):
        self.calls.append(("deny", auth_req_id, decided_by))
        return {"auth_req_id": auth_req_id, "status": "denied", "decided_by": decided_by}

    async def jwks(self):
        return {"keys": []}

    async def close(self):
        pass


@pytest.fixture
def client(monkeypatch):
    fake = FakeIdP()
    monkeypatch.setattr(cpmain, "idp", fake)
    with TestClient(cpmain.app) as c:
        c.fake = fake
        yield c


def test_registration_provisions_identity(client):
    r = client.post("/agents", json={
        "name": "rootcause-db", "kind": "rootcause",
        "identity": {
            "owner_principal": "you@example.com", "trust_level": "low",
            "allowed_envs": ["dev"],
            "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}],
        },
    })
    assert r.status_code == 201
    agent = r.json()
    assert agent["idp_provisioned"] is True
    assert agent["idp_error"] is None
    # The IdP was provisioned under the SAME id the control plane assigned.
    upserts = [c for c in client.fake.calls if c[0] == "upsert_agent"]
    assert len(upserts) == 1
    assert upserts[0][1]["agent_id"] == agent["agent_id"]
    assert upserts[0][1]["owner_principal"] == "you@example.com"


def test_registration_without_identity_skips_idp(client):
    r = client.post("/agents", json={"name": "no-identity"})
    assert r.status_code == 201
    assert r.json()["idp_provisioned"] is False
    assert [c for c in client.fake.calls if c[0] == "upsert_agent"] == []


def test_full_capability_flow_through_broker(client):
    aid = client.post("/agents", json={
        "name": "a", "identity": {"owner_principal": "x", "allowed_envs": ["dev"],
                                   "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}]},
    }).json()["agent_id"]

    tok = client.post(f"/agents/{aid}/identity/token", json={
        "env": "dev", "runtime": {"kind": "cloud", "cluster": "local-dev"}}).json()
    assert tok["access_token"] == "access-xyz"

    grant = client.post(f"/agents/{aid}/identity/grants", json={
        "action": "k8s.get", "resource": "pods/x", "purpose": "rc",
        "reason": "why", "ticket": "INC-1"}).json()
    assert grant["grant_id"] == "grant-1"
    # broker injected the agent_id into the grant
    grant_call = [c for c in client.fake.calls if c[0] == "grant"][0][1]
    assert grant_call["agent_id"] == aid

    cap = client.post("/idp/capabilities/mint", json={
        "agent_access_token": "access-xyz", "grant_id": "grant-1",
        "cap_action": "k8s.get", "cap_resource": "pods/x",
        "purpose": "rc", "reason": "why", "ticket": "INC-1"}).json()
    assert cap["capability_token"] == "cap-xyz"

    ex = client.post("/idp/execute", json={
        "capability_token": "cap-xyz", "tool": "kubernetes",
        "action": "k8s.get", "resource": "pods/x"}).json()
    assert ex["status"] == "executed"


def test_skill_authorize_and_token(client):
    bc = client.post("/idp/skills/authorize", json={
        "skill_id": "demo", "action": "read", "login_hint": "agent:a"}).json()
    assert bc["auth_req_id"] == "ciba-1"
    tok = client.post("/idp/skills/token", params={"auth_req_id": "ciba-1"})
    assert tok.status_code == 200
    assert tok.json()["access_token"] == "skill-tok"


def test_idp_status_and_jwks(client):
    assert client.get("/idp/status").json()["enabled"] is True
    assert client.get("/idp/jwks").json() == {"keys": []}


def test_skill_approval_requires_registered_approver(client):
    # unknown approver -> 404
    r = client.post("/idp/skills/approve", json={
        "auth_req_id": "ciba-1", "approver_agent_id": "nope", "decision": "approve"})
    assert r.status_code == 404

    approver = client.post("/agents", json={"name": "broker", "kind": "approver"}).json()["agent_id"]
    r = client.post("/idp/skills/approve", json={
        "auth_req_id": "ciba-1", "approver_agent_id": approver, "decision": "approve"})
    assert r.status_code == 200 and r.json()["status"] == "approved"
    approve_calls = [c for c in client.fake.calls if c[0] == "approve"]
    assert approve_calls and approve_calls[0][1] == "ciba-1"
    assert approver in approve_calls[0][2]  # decided_by names the approver agent

    r = client.post("/idp/skills/approve", json={
        "auth_req_id": "ciba-2", "approver_agent_id": approver, "decision": "deny"})
    assert r.json()["status"] == "denied"
