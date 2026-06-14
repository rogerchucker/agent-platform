"""Interactive demo — one continuous flow with a live human approval beat.

Unlike demo.sh (two long-running agent processes whose output interleaves), this
runs the whole incident response as ONE linear, narrated flow in a single
terminal, and pauses for a REAL decision when the agent needs a guarded skill:

    SEV0 in Incy
      → responder picks it up (Incy: unacked → investigating)
      → responder needs the guarded `pci-k8s-runbooks` skill, can't self-grant
      → ⏸  YOU (the on-call) approve or deny, right here  ⏸   ← OpenID CIBA
      → on approve: skill granted, agent runs a REAL guarded remediation
        through the IdP tool gateway
      → responder resolves it (Incy: investigating → closed)

Run:
    CONTROL_PLANE=http://sre-control-plane python -u examples/devops/demo_interactive.py
    AUTO_APPROVE=y  ... # non-interactive (for CI/smoke); also accepts 'n' to deny
"""
import asyncio
import os
import sys
import time
import uuid
from pathlib import Path

# Run from anywhere: add the repo root (…/agent-platform) to the import path.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from control_plane.client import ControlPlaneClient

CP = os.environ.get("CONTROL_PLANE", "http://sre-control-plane")
# Reuse the canonical agents (the always-on access broker, and the incident
# responder) when they're already active, instead of creating throwaways.
RESPONDER = os.environ.get("RESPONDER_ID", "devops-incident-responder")
APPROVER = os.environ.get("APPROVER_ID", "devops-access-broker")
SKILL_SUBJECT = "system:serviceaccount:agent-requester:requester"
SKILL, ACTION = "pci-k8s-runbooks", "use"
RUNTIME = {"kind": "cloud", "cluster": "local-dev"}
REMEDIATION_ACTION = "k8s.rollout.restart"
REMEDIATION_RESOURCE = "kubernetes:cde/deploy/checkout"
AUTO_APPROVE = os.environ.get("AUTO_APPROVE")  # 'y'/'n' to skip the prompt

B = "\033[1m"; DIM = "\033[2m"; G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; C = "\033[36m"; M = "\033[35m"; X = "\033[0m"


def say(tag: str, msg: str) -> None:
    print(f"{tag} {msg}")


async def decide(prompt: str) -> bool:
    """The single interactive beat: approve (y) or deny (n)."""
    if AUTO_APPROVE is not None:
        ans = AUTO_APPROVE.strip().lower()
        print(f"{Y}[on-call]{X} {prompt} {DIM}(auto: {ans}){X}")
    else:
        loop = asyncio.get_event_loop()
        ans = (await loop.run_in_executor(None, input, f"{Y}[on-call]{X} {prompt} [y/N] ")).strip().lower()
    return ans in ("y", "yes", "approve", "a")


async def incy_status(cp: ControlPlaneClient, iid: str) -> str:
    try:
        return (await cp.incy_incident(iid)).get("status", "?")
    except Exception:
        return "?"


async def ensure_agent(cp: ControlPlaneClient, agent_id: str, *, name: str, kind: str,
                       trust: str, target: str, capabilities: list, subs: list) -> bool:
    """Reuse the agent if it's already active on the platform; otherwise register
    it (and leave it — we don't tear these down). Returns True if reused."""
    try:
        r = await cp._http.get(f"/agents/{agent_id}")
        status = r.json().get("status") if r.status_code == 200 else None
    except Exception:
        status = None
    if status in ("live", "inactive"):
        cp.agent_id = agent_id
        try:  # nudge liveness for the duration of the demo
            await cp._http.post(f"/agents/{agent_id}/heartbeat")
        except Exception:
            pass
        say(f"{DIM}[setup]{X}", f"reusing active agent {B}{agent_id}{X} (status={status})")
        return True
    await cp.register(
        name=name, kind=kind, agent_id=agent_id, capabilities=capabilities, subscriptions=subs,
        identity={"owner_principal": "rajarshic@gmail.com", "trust_level": trust,
                  "allowed_envs": ["dev"], "framework": "custom", "target_application": target,
                  "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}]})
    say(f"{DIM}[setup]{X}", f"registered {B}{agent_id}{X} (was not active)")
    return False


async def main() -> int:
    async with ControlPlaneClient(CP) as responder, ControlPlaneClient(CP) as approver:
        # ---- setup: reuse the canonical responder + access broker if active ----
        await ensure_agent(responder, RESPONDER, name="DevOps Incident Responder",
                           kind="incident-responder", trust="low", target="incident-response",
                           capabilities=["triage", "runbook-exec"], subs=["incidents"])
        await ensure_agent(approver, APPROVER, name="DevOps Access Broker",
                           kind="approver", trust="high", target="access-broker",
                           capabilities=["skill-approval"], subs=["access.requests"])

        # ---- 1. a SEV0 fires in Incy ----
        summary = f"Checkout pods CrashLoopBackOff in CDE ({uuid.uuid4().hex[:4]})"
        ticket = f"INC-{uuid.uuid4().hex[:6]}"
        iid = None
        try:
            await responder.incy_trigger(summary, severity="critical", dedup_key=f"demo-{int(time.time())}")
            for _ in range(10):
                m = [i for i in await responder.incy_incidents(status="triggered", limit=30)
                     if i["title"] == summary]
                if m:
                    iid = m[0]["id"]
                    break
                await asyncio.sleep(1)
        except Exception as exc:
            say(f"{R}[incy]{X}", f"(incy unavailable: {exc} — continuing with the CIBA flow)")
        say(f"{M}[incy]{X}", f"SEV0 raised: {summary!r}" + (f"  status={await incy_status(responder, iid)} (UNACKED)" if iid else ""))

        # ---- 2. responder picks it up → Incy investigating ----
        if iid:
            await responder.publish("incidents", {
                "kind": "status", "incident_id": iid, "status": "investigating",
                "agent_id": responder.agent_id, "agent_name": "DevOps Incident Responder",
                "note": f"Picked up for {ticket} — investigating root cause"})
            await asyncio.sleep(3)
            say(f"{M}[incy]{X}", f"status → {await incy_status(responder, iid)} (INVESTIGATING, picked up by agent)")

        # ---- 3. responder needs the guarded runbook → must escalate ----
        bc = await responder.authorize_skill(SKILL, action=ACTION, login_hint=SKILL_SUBJECT,
                                             binding_message=f"runbook access for {ticket}",
                                             reason=f"remediate {summary}")
        auth_req_id = bc["auth_req_id"]
        st, body = await responder.poll_skill_token(auth_req_id)
        say(f"{C}[agent]{X}", f"needs {B}{SKILL}:{ACTION}{X} — self-grant {R}BLOCKED{X} "
                              f"({body.get('error')}); escalating to on-call (CIBA)")

        # ---- 4. THE interactive beat: human approves or denies, in this flow ----
        approved = await decide(f"Approve {SKILL}:{ACTION} for {ticket} ({summary})?")
        if approved:
            dec = await approver.approve_skill(auth_req_id, reason=f"on-call approves {ticket}")
            say(f"{G}[on-call]{X}", f"APPROVED → {dec.get('status')} (decided_by {dec.get('decided_by')})")
            tokb = await responder.mint_skill_token(auth_req_id)
            intro = await responder.introspect(tokb["access_token"])
            say(f"{C}[agent]{X}", f"received runbook skill token (active={intro['active']}, "
                                  f"grant={tokb.get('grant_id')})")

            # ---- 5. REAL guarded remediation through the IdP gateway ----
            try:
                tok = await responder.get_access_token(runtime=RUNTIME, env="dev",
                                                       session_id=ticket, trace_id=ticket)
                grant = await responder.request_grant(
                    action=REMEDIATION_ACTION, resource=REMEDIATION_RESOURCE,
                    purpose="remediation", reason=f"auto-remediation for {ticket}", ticket=ticket)
                cap = await responder.mint_capability(
                    tok, grant["grant_id"], REMEDIATION_ACTION, REMEDIATION_RESOURCE,
                    purpose="remediation", reason=f"auto-remediation for {ticket}", ticket=ticket)
                out = await responder.execute(cap["capability_token"], tool="kubernetes",
                                              action=REMEDIATION_ACTION, resource=REMEDIATION_RESOURCE,
                                              params={"namespace": "cde", "workload": "checkout"})
                say(f"{G}[agent]{X}", f"🔧 gateway executed {REMEDIATION_ACTION} on "
                                      f"{REMEDIATION_RESOURCE} → {out.get('status')}")
                outcome = "remediated using pci-k8s-runbooks"
            except Exception as exc:
                say(f"{R}[agent]{X}", f"gateway remediation failed: {exc}")
                outcome = "runbook granted; remediation errored"
        else:
            await approver.deny_skill(auth_req_id, reason=f"on-call denies {ticket}")
            say(f"{R}[on-call]{X}", "DENIED — agent proceeds with limited triage only")
            outcome = "triaged without runbook access (access denied)"

        # ---- 6. resolve → Incy closed ----
        if iid:
            await responder.publish("incidents", {
                "kind": "status", "incident_id": iid, "status": "closed",
                "agent_id": responder.agent_id, "agent_name": "DevOps Incident Responder",
                "note": f"{outcome}; closing {ticket}"})
            await asyncio.sleep(3)
            say(f"{M}[incy]{X}", f"status → {await incy_status(responder, iid)} (CLOSED)  outcome: {outcome}")

        # No teardown: these are the canonical, reused agents — leave them in place
        # for the next run (and so the always-on broker keeps serving).
        print(f"\n{G}done.{X}")
        return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
