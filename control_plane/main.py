"""SRE Agent Control Plane — FastAPI application.

Endpoints
---------
REST
  POST   /agents                  register an agent
  DELETE /agents/{id}             deregister
  POST   /agents/{id}/heartbeat   keep-alive (sets status -> live)
  GET    /agents                  list agents + status
  GET    /agents/{id}             one agent
  POST   /messages                publish a message to a topic
  GET    /messages                recent message history (optional ?topic=)
  GET    /topics                  known topics
  GET    /healthz                 liveness probe

WebSocket
  /ws/{agent_id}?topics=a,b       full-duplex: receive subscribed messages,
                                  send {"type":"publish"|"heartbeat"|"subscribe"}

UI
  GET    /                        live dashboard
"""
from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .idp import IdPClient, IdPError
from .incy import INCIDENTS_TOPIC, IncyConnector
from .message_queue import MessageQueue
from .models import (
    Agent,
    CapabilityRequest,
    ExecuteRequest,
    GrantRequest,
    IdentitySpec,
    IntrospectRequest,
    Message,
    PublishRequest,
    RegisterRequest,
    SkillApprovalRequest,
    SkillAuthorizeRequest,
    TokenRequest,
)
from .registry import AgentRegistry

HEARTBEAT_TIMEOUT = 30.0
STATIC_DIR = Path(__file__).parent / "static"

registry = AgentRegistry(heartbeat_timeout=HEARTBEAT_TIMEOUT)
mq = MessageQueue()
idp = IdPClient()  # configured via IDP_BASE_URL / IDP_ADMIN_API_KEY / IDP_INTERNAL_API_KEY
incy = IncyConnector(mq)  # configured via INCY_BASE_URL / INCY_INTEGRATION_KEY / INCY_AGENT_USER_ID


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    registry.start()
    await incy.start()
    yield
    await registry.stop()
    await incy.stop()
    await idp.close()


def _provision_payload(agent_id: str, spec: IdentitySpec) -> dict:
    """Build the IdP /agents body from a registration identity spec."""
    return {
        "agent_id": agent_id,
        "tenant": spec.tenant,
        "owner_principal": spec.owner_principal,
        "framework": spec.framework,
        "target_application": spec.target_application,
        "trust_level": spec.trust_level,
        "allowed_envs": spec.allowed_envs,
        "runtime_bindings": [rb.model_dump() for rb in spec.runtime_bindings],
        "status": "active",
    }


app = FastAPI(title="SRE Agent Control Plane", version="0.1.0", lifespan=lifespan)


# -- agent registration & status ------------------------------------------
@app.post("/agents", status_code=201)
async def register_agent(req: RegisterRequest):
    agent = await registry.register(req)
    # Registration → IdP provisioning: if an identity spec was supplied and the
    # IdP is configured, provision a matching identity under the same agent_id.
    if req.identity is not None and idp.enabled:
        try:
            await idp.upsert_agent(_provision_payload(agent.agent_id, req.identity))
            agent.idp_provisioned = True
            agent.idp_error = None
        except IdPError as exc:
            agent.idp_provisioned = False
            agent.idp_error = f"{exc.status}: {exc.detail}"
    return agent.model_dump()


def _require_idp() -> None:
    if not idp.enabled:
        raise HTTPException(503, "idp_not_configured")


def _agent_or_404(agent_id: str) -> Agent:
    agent = registry.get(agent_id)
    if not agent:
        raise HTTPException(404, "agent not found")
    return agent


# -- IdP broker: identity, grants, capabilities, execution -----------------
@app.get("/idp/status")
async def idp_status():
    return {"enabled": idp.enabled, "base_url": idp.base_url or None}


@app.get("/idp/jwks")
async def idp_jwks():
    _require_idp()
    return await idp.jwks()


@app.get("/agents/{agent_id}/identity")
async def get_identity(agent_id: str):
    _require_idp()
    _agent_or_404(agent_id)
    try:
        return await idp.get_agent(agent_id)
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.post("/agents/{agent_id}/identity/token")
async def identity_token(agent_id: str, req: TokenRequest):
    """Exchange a runtime attestation for an agent access token (brokered)."""
    _require_idp()
    _agent_or_404(agent_id)
    att = {
        "kind": req.runtime.kind,
        "cluster": req.runtime.cluster,
        "namespace": req.runtime.namespace,
        "service_account": req.runtime.service_account,
        "spiffe_id": req.runtime.spiffe_id,
        "agent_id": agent_id,
        "env": req.env,
        "session_id": req.session_id,
        "trace_id": req.trace_id,
    }
    try:
        return await idp.attest_exchange(att)
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.post("/agents/{agent_id}/identity/grants")
async def identity_grant(agent_id: str, req: GrantRequest):
    _require_idp()
    _agent_or_404(agent_id)
    grant = {**req.model_dump(), "agent_id": agent_id}
    try:
        return await idp.create_grant(grant)
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.post("/idp/capabilities/mint")
async def mint_capability(req: CapabilityRequest):
    _require_idp()
    try:
        return await idp.mint_capability(req.model_dump())
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.post("/idp/execute")
async def gateway_execute(req: ExecuteRequest):
    _require_idp()
    try:
        return await idp.gateway_execute(req.model_dump())
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.post("/idp/introspect")
async def introspect(req: IntrospectRequest):
    _require_idp()
    try:
        return await idp.introspect(req.token)
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


# -- IdP broker: skill access (CIBA) ---------------------------------------
@app.post("/idp/skills/authorize")
async def skill_authorize(req: SkillAuthorizeRequest):
    _require_idp()
    body = {
        "scope": f"skill:{req.skill_id}:{req.action}",
        "login_hint": req.login_hint,
        "binding_message": req.binding_message,
        "reason": req.reason,
    }
    try:
        return await idp.bc_authorize(body)
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.post("/idp/skills/token")
async def skill_token(auth_req_id: str):
    """Poll for a skill-access token. Returns the IdP's CIBA body verbatim
    (including {error: authorization_pending|slow_down} while awaiting approval)."""
    _require_idp()
    try:
        status, body = await idp.ciba_token(auth_req_id)
        return JSONResponse(status_code=status, content=body)
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.post("/idp/skills/approve")
async def skill_approve(req: SkillApprovalRequest):
    """An approver agent decides a pending skill request. The control plane
    verifies the approver is a registered, live agent, then brokers the
    decision to the IdP with the admin key (agents never hold that key)."""
    _require_idp()
    approver = _agent_or_404(req.approver_agent_id)
    decided_by = f"agent:{approver.agent_id}({approver.name})"
    try:
        if req.decision == "approve":
            return await idp.approve(req.auth_req_id, decided_by=decided_by, reason=req.reason)
        return await idp.deny(req.auth_req_id, decided_by=decided_by, reason=req.reason)
    except IdPError as exc:
        raise HTTPException(exc.status if exc.status < 600 else 502, exc.detail)


@app.delete("/agents/{agent_id}")
async def deregister_agent(agent_id: str):
    if not await registry.deregister(agent_id):
        raise HTTPException(404, "agent not found")
    await mq.unsubscribe(agent_id)
    return {"ok": True}


@app.post("/agents/{agent_id}/heartbeat")
async def heartbeat(agent_id: str):
    agent = await registry.heartbeat(agent_id)
    if not agent:
        raise HTTPException(404, "agent not found or deregistered")
    return {"status": agent.status, "last_heartbeat": agent.last_heartbeat}


@app.get("/agents")
async def list_agents(include_deregistered: bool = False):
    agents = registry.list(include_deregistered=include_deregistered)
    return {"agents": [a.model_dump() for a in agents], "count": len(agents)}


@app.get("/agents/{agent_id}")
async def get_agent(agent_id: str):
    agent = registry.get(agent_id)
    if not agent:
        raise HTTPException(404, "agent not found")
    return agent.model_dump()


# -- message queue ----------------------------------------------------------
@app.post("/messages", status_code=201)
async def publish_message(req: PublishRequest):
    msg = Message(topic=req.topic, payload=req.payload, sender=req.sender or "control-plane")
    delivered = await mq.publish(msg)
    return {"message": msg.model_dump(), "delivered": delivered}


@app.get("/messages")
async def get_messages(topic: str | None = None, limit: int = 50):
    msgs = mq.history(topic=topic, limit=limit)
    return {"messages": [m.model_dump() for m in msgs], "count": len(msgs)}


@app.get("/topics")
async def list_topics():
    return {"topics": mq.topics(), "subscribers": mq.subscriber_count()}


@app.get("/healthz")
async def healthz():
    return {"ok": True}


# -- incy connector --------------------------------------------------------
@app.get("/incy/status")
async def incy_status():
    return {"enabled": incy.enabled, "base_url": incy.base_url or None,
            "incidents_topic": INCIDENTS_TOPIC}


@app.get("/incy/incidents")
async def incy_incidents(status: str | None = None, limit: int = 20):
    if not incy.enabled:
        raise HTTPException(503, "incy_not_configured")
    return {"incidents": await incy.list_incidents(status=status, limit=limit)}


@app.get("/incy/incidents/{incident_id}")
async def incy_incident(incident_id: str):
    if not incy.enabled:
        raise HTTPException(503, "incy_not_configured")
    try:
        return await incy.get_incident(incident_id)
    except Exception as exc:
        raise HTTPException(502, f"incy_error: {exc}")


@app.post("/incy/trigger", status_code=201)
async def incy_trigger(summary: str, severity: str = "warning", dedup_key: str | None = None):
    """Create a real incy incident (demo helper). The connector's poller then
    surfaces it onto the incidents topic for agents to pick up."""
    if not incy.enabled:
        raise HTTPException(503, "incy_not_configured")
    try:
        return await incy.trigger_event(summary, severity=severity, dedup_key=dedup_key)
    except Exception as exc:
        raise HTTPException(502, f"incy_error: {exc}")


# -- WebSocket: live listen + post -----------------------------------------
@app.websocket("/ws/{agent_id}")
async def agent_ws(websocket: WebSocket, agent_id: str, topics: str = Query(default="")):
    await websocket.accept()

    agent = registry.get(agent_id)
    if not agent:
        await websocket.send_json({"type": "error", "error": "unknown agent_id; register first"})
        await websocket.close(code=4404)
        return

    topic_list = [t.strip() for t in topics.split(",") if t.strip()] or agent.subscriptions
    sub = await mq.subscribe(agent_id, topic_list, replay=0)
    await registry.set_connected(agent_id, True)
    await websocket.send_json({"type": "ready", "agent_id": agent_id, "topics": list(sub.topics)})

    async def pump_outbound():
        """Drain the agent's queue to the socket."""
        while True:
            msg = await sub.queue.get()
            await websocket.send_json({"type": "message", **msg.model_dump()})

    outbound = asyncio.create_task(pump_outbound())
    try:
        while True:
            raw = await websocket.receive_text()
            await _handle_ws_frame(agent_id, raw, websocket)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # pragma: no cover - defensive
        with contextlib.suppress(Exception):
            await websocket.send_json({"type": "error", "error": str(exc)})
    finally:
        outbound.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await outbound
        await registry.set_connected(agent_id, False)
        await mq.unsubscribe(agent_id)


async def _handle_ws_frame(agent_id: str, raw: str, websocket: WebSocket) -> None:
    try:
        frame = json.loads(raw)
    except json.JSONDecodeError:
        await websocket.send_json({"type": "error", "error": "invalid JSON"})
        return

    ftype = frame.get("type", "publish")
    if ftype == "heartbeat":
        await registry.heartbeat(agent_id)
        await websocket.send_json({"type": "heartbeat_ack"})
    elif ftype == "subscribe":
        await mq.update_topics(agent_id, frame.get("topics", []))
        await websocket.send_json({"type": "subscribed", "topics": frame.get("topics", [])})
    elif ftype == "publish":
        topic = frame.get("topic")
        if not topic:
            await websocket.send_json({"type": "error", "error": "publish requires 'topic'"})
            return
        msg = Message(topic=topic, payload=frame.get("payload", {}), sender=agent_id)
        delivered = await mq.publish(msg)
        await registry.heartbeat(agent_id)  # publishing proves liveness
        await websocket.send_json(
            {"type": "publish_ack", "message_id": msg.message_id, "delivered": delivered}
        )
    else:
        await websocket.send_json({"type": "error", "error": f"unknown frame type: {ftype}"})


# -- dashboard --------------------------------------------------------------
@app.get("/")
async def dashboard():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse({"service": "sre-agent-control-plane", "ui": "missing"})


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
