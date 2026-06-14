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

import json
import os

from control_plane.client import ControlPlaneClient

# A generic incident responder: it picks incidents off the platform, escalates
# for a guarded skill when it needs one, and (once granted) performs a guarded
# remediation through the IdP gateway. Everything that ties it to a particular
# use case — the skill it needs, the workload identity it attests with, and the
# remediation it runs — is configuration (env), not hardcoded. The defaults below
# happen to match the SRE/PCI demo, but the agent is not specific to it.
CONTROL_PLANE = os.environ.get("CONTROL_PLANE", "http://sre-control-plane")
AGENT_ID = os.environ.get("AGENT_ID", "devops-incident-responder")
AGENT_NAME = os.environ.get("AGENT_NAME", "DevOps Incident Responder")
AGENT_KIND = os.environ.get("AGENT_KIND", "incident-responder")
# A clone is already provisioned in the IdP by the /clone call, so it joins the
# queue WITHOUT re-provisioning identity (which would re-bind its runtime).
SKIP_IDENTITY = os.environ.get("SKIP_IDENTITY") == "1"

# What guarded skill this responder needs, and the subject the IdP allow-lists
# for it (see SKILL_CLIENT_ALLOWLIST). Empty INCIDENT_SKILL → skip escalation.
NEEDED_SKILL = os.environ.get("INCIDENT_SKILL", "pci-k8s-runbooks")
NEEDED_ACTION = os.environ.get("INCIDENT_SKILL_ACTION", "use")
SKILL_SUBJECT = os.environ.get("SKILL_SUBJECT", "system:serviceaccount:agent-requester:requester")

# Workload identity to attest with, and the guarded remediation to run through
# the IdP tool gateway (a real capability mint + execute, not a narration).
AGENT_ENV = os.environ.get("AGENT_ENV", "dev")
RUNTIME = {"kind": os.environ.get("RUNTIME_KIND", "cloud"),
           "cluster": os.environ.get("RUNTIME_CLUSTER", "local-dev")}
REMEDIATION_TOOL = os.environ.get("REMEDIATION_TOOL", "kubernetes")
REMEDIATION_ACTION = os.environ.get("REMEDIATION_ACTION", "k8s.rollout.restart")
REMEDIATION_RESOURCE = os.environ.get("REMEDIATION_RESOURCE", "kubernetes:cde/deploy/checkout")
REMEDIATION_PARAMS = json.loads(os.environ.get("REMEDIATION_PARAMS", "{}"))

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

    # 2. If this responder needs a guarded skill it can't self-grant, escalate for
    #    approval (BLOCKED, UI red). If no skill is configured, it proceeds straight
    #    to remediation — the agent isn't tied to any specific guarded workflow.
    granted = denied = False
    if not NEEDED_SKILL:
        granted = True
    else:
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
            if grant.get("denied"):
                denied = True
                print(f"  ⛔ access DENIED by approver ({grant.get('reason', 'no reason given')})")
            else:
                granted = True
                await cp.set_work_state("investigating", note="access granted; remediating")
                intro = await cp.introspect(grant["skill_access_token"])
                print(f"  ✅ got runbook access (token active={intro['active']})")
        except asyncio.TimeoutError:
            print("  ⚠ no approval within timeout")
        finally:
            grants.pop(ticket, None)

    # 2b. Remediate through the IdP gateway — ONLY if access was granted.
    remediated = await remediate_via_gateway(cp, ticket) if granted else False

    # 3. Resolve ONLY when we actually remediated. A denied / timed-out / failed
    #    agent must NOT auto-close the incident — leave it OPEN for a human.
    if remediated:
        await set_status(cp, incident_id, "closed",
                         f"Remediated with {NEEDED_SKILL}; closing {ticket}")
        await cp.set_work_state("idle", note=f"resolved {ticket}")
        print(f"  → wrote 'closed' to '{INCIDENTS_TOPIC}' (incy: RESOLVED)")
    else:
        reason = ("access denied by approver" if denied
                  else "approval timed out" if not granted
                  else "remediation failed")
        await set_status(cp, incident_id, "escalated",
                         f"⚠ {ticket}: agent could NOT remediate ({reason}) — needs a human")
        await cp.set_work_state("idle", note=f"handed off {ticket} to a human")
        print(f"  → left incident OPEN for a human ({reason}); NOT resolved")


async def remediate_via_gateway(cp: ControlPlaneClient, ticket: str) -> bool:
    """Mint a scoped capability and execute the remediation through the IdP
    gateway. Returns True on success. A decommissioned/unattestable agent is
    denied here — which is exactly the point."""
    try:
        token = await cp.get_access_token(runtime=RUNTIME, env=AGENT_ENV,
                                          session_id=ticket, trace_id=ticket)
        grant = await cp.request_grant(
            action=REMEDIATION_ACTION, resource=REMEDIATION_RESOURCE, env=AGENT_ENV,
            purpose="remediation", reason=f"auto-remediation for {ticket}", ticket=ticket)
        cap = await cp.mint_capability(
            token, grant["grant_id"], REMEDIATION_ACTION, REMEDIATION_RESOURCE,
            purpose="remediation", reason=f"auto-remediation for {ticket}", ticket=ticket)
        out = await cp.execute(cap["capability_token"], tool=REMEDIATION_TOOL,
                               action=REMEDIATION_ACTION, resource=REMEDIATION_RESOURCE,
                               params=REMEDIATION_PARAMS)
        print(f"  🔧 gateway executed {REMEDIATION_ACTION} on {REMEDIATION_RESOURCE} "
              f"→ {out.get('status')}")
        return out.get("status") == "executed"
    except Exception as exc:
        print(f"  (gateway remediation unavailable: {exc})")
        return False


async def main():
    grants: dict = {}
    async with ControlPlaneClient(CONTROL_PLANE) as cp:
        identity = None if SKIP_IDENTITY else {
            "owner_principal": os.environ.get("AGENT_OWNER", "rajarshic@gmail.com"),
            "trust_level": os.environ.get("AGENT_TRUST", "low"),
            "allowed_envs": [AGENT_ENV], "framework": "custom",
            "target_application": "incident-response",
            "runtime_bindings": [RUNTIME]}
        agent = await cp.register(
            name=AGENT_NAME, kind=AGENT_KIND, agent_id=AGENT_ID,
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
