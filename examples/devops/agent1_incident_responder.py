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

import os

from control_plane.client import ControlPlaneClient

CONTROL_PLANE = os.environ.get("CONTROL_PLANE", "http://sre-control-plane")
# Parameterized so a CLONE can run as a second instance under its own agent_id.
AGENT_ID = os.environ.get("AGENT_ID", "devops-incident-responder")
AGENT_NAME = os.environ.get("AGENT_NAME", "DevOps Incident Responder")
# A clone is already provisioned in the IdP by the /clone call, so it joins the
# queue WITHOUT re-provisioning identity (which would re-bind its runtime).
SKIP_IDENTITY = os.environ.get("SKIP_IDENTITY") == "1"
# Subject the IdP allow-lists for guarded skills (see SKILL_CLIENT_ALLOWLIST).
SKILL_SUBJECT = "system:serviceaccount:agent-requester:requester"

NEEDED_SKILL = "pci-k8s-runbooks"
NEEDED_ACTION = "use"

# Real guarded remediation through the IdP tool gateway — the agent mints a
# capability and executes it, instead of only narrating "ran the runbook".
RUNTIME = {"kind": "cloud", "cluster": "local-dev"}
REMEDIATION_ACTION = "k8s.rollout.restart"
REMEDIATION_RESOURCE = "kubernetes:cde/deploy/checkout"

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
        "agent_name": AGENT_NAME,
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

    # 1. Pick it up → incy goes unacked -> investigating (UI: green "investigating").
    await cp.set_work_state("investigating", note=f"picked up {ticket}")
    await set_status(cp, incident_id, "investigating",
                     f"Picked up by SRE agent for {ticket} — investigating root cause")
    print(f"  → picked up; wrote 'investigating' to '{INCIDENTS_TOPIC}' (incy: acknowledged)")

    # 2. Investigate: need the guarded runbook skill, which we can't self-grant.
    #    While waiting on approval the agent is BLOCKED (UI: red).
    await prove_no_access(cp)
    print(f"  escalating on '{REQUESTS_TOPIC}' for {NEEDED_SKILL}:{NEEDED_ACTION} …")
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    grants[ticket] = fut
    await cp.publish(REQUESTS_TOPIC, {
        "ticket": ticket, "requester_agent_id": cp.agent_id, "requester_subject": SKILL_SUBJECT,
        "skill": NEEDED_SKILL, "action": NEEDED_ACTION, "incident": inc, "deliver_to": GRANTS_TOPIC,
    })
    await cp.set_work_state("blocked", note=f"awaiting approval for {NEEDED_SKILL}")
    try:
        grant = await asyncio.wait_for(fut, timeout=ESCALATION_TIMEOUT)
        # approved → back to investigating/remediating (UI: green again)
        await cp.set_work_state("investigating", note="access granted; remediating")
        intro = await cp.introspect(grant["skill_access_token"])
        print(f"  ✅ got runbook access (token active={intro['active']}); ran the runbook")
        outcome = "remediated using pci-k8s-runbooks"
    except asyncio.TimeoutError:
        print("  ⚠ no grant within timeout — proceeding with limited triage")
        outcome = "triaged without runbook access"
    finally:
        grants.pop(ticket, None)

    # 2b. Actually remediate through the IdP tool gateway (a real guarded action,
    #     not a narrated one). Needs a runtime the agent can attest with.
    await remediate_via_gateway(cp, ticket)

    # 3. Done → incy goes investigating -> closed; agent goes idle (UI: not "working").
    await set_status(cp, incident_id, "closed", f"{outcome}; closing {ticket}")
    await cp.set_work_state("idle", note=f"closed {ticket}")
    print(f"  → wrote 'closed' to '{INCIDENTS_TOPIC}' (incy: resolved)")


async def remediate_via_gateway(cp: ControlPlaneClient, ticket: str) -> None:
    """Mint a scoped capability and execute the remediation through the IdP
    gateway. Best-effort: an agent with no attestable runtime (or a
    decommissioned one) is denied here — which is exactly the point."""
    try:
        token = await cp.get_access_token(runtime=RUNTIME, env="dev",
                                          session_id=ticket, trace_id=ticket)
        grant = await cp.request_grant(
            action=REMEDIATION_ACTION, resource=REMEDIATION_RESOURCE,
            purpose="remediation", reason=f"auto-remediation for {ticket}", ticket=ticket)
        cap = await cp.mint_capability(
            token, grant["grant_id"], REMEDIATION_ACTION, REMEDIATION_RESOURCE,
            purpose="remediation", reason=f"auto-remediation for {ticket}", ticket=ticket)
        out = await cp.execute(cap["capability_token"], tool="kubernetes",
                               action=REMEDIATION_ACTION, resource=REMEDIATION_RESOURCE,
                               params={"namespace": "cde", "workload": "checkout"})
        print(f"  🔧 gateway executed {REMEDIATION_ACTION} on {REMEDIATION_RESOURCE} "
              f"→ {out.get('status')}")
    except Exception as exc:
        print(f"  (gateway remediation unavailable: {exc})")


async def main():
    grants: dict = {}
    async with ControlPlaneClient(CONTROL_PLANE) as cp:
        identity = None if SKIP_IDENTITY else {
            "owner_principal": "rajarshic@gmail.com", "trust_level": "low",
            "allowed_envs": ["dev"], "framework": "custom",
            "target_application": "incident-response",
            "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}]}
        agent = await cp.register(
            name=AGENT_NAME, kind="incident-responder", agent_id=AGENT_ID,
            capabilities=["triage", "runbook-exec"],
            subscriptions=[INCIDENTS_TOPIC, GRANTS_TOPIC],
            identity=identity,
        )
        print(f"registered {agent['agent_id']} (idp_provisioned={agent['idp_provisioned']})")
        print(f"listening for incy incidents on '{INCIDENTS_TOPIC}' …")
        async for msg in cp.listen_resilient([INCIDENTS_TOPIC, GRANTS_TOPIC]):
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
