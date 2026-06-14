"""Agent 1 — DevOps Incident Responder (the SRE agent).

Story
-----
* Registers with the platform for the first time (identity provisioned).
* Subscribes to the shared **incidents** topic — the same topic incy publishes
  new incidents on (via the control plane's incy connector).
* When incy raises an incident, the agent **picks it up**: it writes a status
  update back to that *same* topic (``investigating``), which the connector
  applies to incy so its UI flips the incident from *unacked* → *investigating*
  and records that the agent picked it up.
* It then investigates. The needed runbook skill (`pci-k8s-runbooks`) is
  *guarded*, so the agent has **no access** and must escalate: it asks the
  access-broker on ``access.requests`` and waits on the ticket (grant arrives on
  its private grant topic). [Run agent2 to satisfy this; otherwise it proceeds
  after a short wait.]
* When the investigation is done it writes ``closed`` to the incidents topic →
  the connector resolves the incident in incy (*closed*).

So the three incident states surfaced on incy's UI are driven by this agent:
  unacked (incy) → investigating (picked up) → closed (resolved).

Run:
    python -u examples/devops/agent1_incident_responder.py
"""
import asyncio
import uuid

from control_plane.client import ControlPlaneClient

CONTROL_PLANE = "http://sre-control-plane"
AGENT_ID = "devops-incident-responder"
# Subject the IdP allow-lists for guarded skills (see SKILL_CLIENT_ALLOWLIST).
SKILL_SUBJECT = "system:serviceaccount:agent-requester:requester"

NEEDED_SKILL = "pci-k8s-runbooks"
NEEDED_ACTION = "use"

INCIDENTS_TOPIC = "incidents"        # incy publishes here; we read + write status here
REQUESTS_TOPIC = "access.requests"   # we ask the broker for escalation here
GRANTS_TOPIC = f"access.grants.{AGENT_ID}"  # broker delivers the token here

ESCALATION_TIMEOUT = 25.0            # proceed/close even if no broker is running


async def set_status(cp: ControlPlaneClient, incident_id: str, status: str, note: str) -> None:
    """Write an incident status update to the shared topic (the connector
    applies it to incy: investigating→acknowledge, closed→resolve)."""
    await cp.publish(INCIDENTS_TOPIC, {
        "kind": "status",
        "incident_id": incident_id,
        "status": status,
        "agent_id": cp.agent_id,
        "agent_name": "DevOps Incident Responder",
        "note": note,
    })


async def prove_no_access(cp: ControlPlaneClient) -> bool:
    bc = await cp.authorize_skill(NEEDED_SKILL, action=NEEDED_ACTION, login_hint=SKILL_SUBJECT,
                                  reason="self-check before escalation")
    status, body = await cp.poll_skill_token(bc["auth_req_id"])
    blocked = status != 200
    print(f"  [self-check] {NEEDED_SKILL}:{NEEDED_ACTION} -> "
          f"{'BLOCKED (' + body.get('error', '?') + ')' if blocked else 'granted'}")
    return blocked


async def handle_incident(cp: ControlPlaneClient, inc: dict, grants: dict) -> None:
    incident_id = inc["incident_id"]
    num = inc.get("incident_number")
    ticket = f"INC-{uuid.uuid4().hex[:6]}"
    print(f"\n[incy incident #{num}] {inc.get('title')!r} (sev={inc.get('severity')})")

    # 1. Pick it up → incy goes unacked -> investigating.
    await set_status(cp, incident_id, "investigating",
                     f"Picked up by SRE agent for {ticket} — investigating root cause")
    print(f"  → picked up; wrote 'investigating' to '{INCIDENTS_TOPIC}' (incy: acknowledged)")

    # 2. Investigate: need the guarded runbook skill, which we can't self-grant.
    await prove_no_access(cp)
    print(f"  escalating on '{REQUESTS_TOPIC}' for {NEEDED_SKILL}:{NEEDED_ACTION} …")
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    grants[ticket] = fut
    await cp.publish(REQUESTS_TOPIC, {
        "ticket": ticket, "requester_agent_id": cp.agent_id, "requester_subject": SKILL_SUBJECT,
        "skill": NEEDED_SKILL, "action": NEEDED_ACTION, "incident": inc, "deliver_to": GRANTS_TOPIC,
    })
    try:
        grant = await asyncio.wait_for(fut, timeout=ESCALATION_TIMEOUT)
        intro = await cp.introspect(grant["skill_access_token"])
        print(f"  ✅ got runbook access (token active={intro['active']}); ran the runbook")
        outcome = "remediated using pci-k8s-runbooks"
    except asyncio.TimeoutError:
        print("  ⚠ no grant within timeout — proceeding with limited triage")
        outcome = "triaged without runbook access"
    finally:
        grants.pop(ticket, None)

    # 3. Done → incy goes investigating -> closed.
    await set_status(cp, incident_id, "closed", f"{outcome}; closing {ticket}")
    print(f"  → wrote 'closed' to '{INCIDENTS_TOPIC}' (incy: resolved)")


async def main():
    grants: dict = {}
    async with ControlPlaneClient(CONTROL_PLANE) as cp:
        agent = await cp.register(
            name="DevOps Incident Responder", kind="incident-responder", agent_id=AGENT_ID,
            capabilities=["triage", "runbook-exec"],
            subscriptions=[INCIDENTS_TOPIC, GRANTS_TOPIC],
            identity={"owner_principal": "rajarshic@gmail.com", "trust_level": "low",
                      "allowed_envs": ["dev"], "framework": "custom",
                      "target_application": "incident-response",
                      "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}]},
        )
        print(f"registered {agent['agent_id']} (idp_provisioned={agent['idp_provisioned']})")
        print(f"listening for incy incidents on '{INCIDENTS_TOPIC}' …")
        async for msg in cp.listen([INCIDENTS_TOPIC, GRANTS_TOPIC]):
            payload = msg["payload"]
            if msg["topic"] == INCIDENTS_TOPIC and payload.get("kind") == "incident":
                # handle concurrently so one slow investigation doesn't block others
                asyncio.create_task(handle_incident(cp, payload, grants))
            elif msg["topic"] == GRANTS_TOPIC:
                fut = grants.get(payload.get("ticket"))
                if fut and not fut.done():
                    fut.set_result(payload)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
