"""Agent 2 — DevOps Access Broker (the *approver* agent).

Story
-----
* Registers with the platform for the first time (identity provisioned).
* Listens on a **separate** queue topic from incy — ``access.requests`` — where
  blocked agents ask for escalation (incy posts to ``alerts.incy``).
* On a request it **finds the blocked agent in the platform's running-agents
  list**, then **mints a skill-access token through the platform using OpenID
  CIBA** and **grants it directly to the requesting agent** over the queue:

    bc-authorize (login_hint = requester)  →  auth_req_id  (guarded ⇒ pending)
    approve (as the authorized approver)    →  approved
    poll /token                             →  skill-access token
    publish token to the requester's topic  →  direct hand-off

Run this first, then start Agent 1:
    python examples/devops/agent2_access_broker.py
"""
import asyncio
import os

from control_plane.client import ControlPlaneClient

CONTROL_PLANE = os.environ.get("CONTROL_PLANE", "http://sre-control-plane")
AGENT_ID = "devops-access-broker"
REQUESTS_TOPIC = "access.requests"   # listens here (separate from incy's alerts.incy)

# Demo pacing for the approval beat:
#   BROKER_MANUAL=1          → wait for the operator to press Enter before approving
#   BROKER_APPROVAL_DELAY=N  → pause N seconds before approving (default 0 = instant)
MANUAL = os.getenv("BROKER_MANUAL") == "1"
APPROVAL_DELAY = float(os.getenv("BROKER_APPROVAL_DELAY", "0"))


async def _approval_gate(ticket: str) -> None:
    """Make the approval a visible, deliberate action by the access broker."""
    if MANUAL:
        print(f"  ⏳ request {ticket} is PENDING — press Enter to approve as access broker…")
        await asyncio.get_event_loop().run_in_executor(None, input)
    elif APPROVAL_DELAY > 0:
        print(f"  ⏳ request {ticket} is PENDING — reviewing… (approving in {APPROVAL_DELAY:.0f}s)")
        await asyncio.sleep(APPROVAL_DELAY)


async def handle_request(cp: ControlPlaneClient, req: dict) -> None:
    ticket = req.get("ticket")
    requester_id = req.get("requester_agent_id")
    subject = req.get("requester_subject")
    skill, action = req["skill"], req["action"]
    deliver_to = req["deliver_to"]
    print(f"\n[access request] ticket={ticket} from agent={requester_id} "
          f"wants {skill}:{action}")

    # 1. Find the blocked agent in the platform's list of running agents.
    agents = await cp.list_agents()
    match = next((a for a in agents if a["agent_id"] == requester_id), None)
    if not match:
        print(f"  ✗ requester {requester_id} not found among running agents — ignoring")
        return
    print(f"  found requester: name='{match['name']}' status={match['status']} "
          f"kind={match['kind']}")
    if match["status"] != "live":
        print("  ✗ requester is not live — denying escalation")
        return

    # 2. Mint a token via OpenID CIBA, on behalf of the requester's subject.
    bc = await cp.authorize_skill(skill, action=action, login_hint=subject,
                                  binding_message=f"escalation for {ticket}",
                                  reason=f"approved by broker for {ticket}")
    auth_req_id = bc["auth_req_id"]
    print(f"  bc-authorize -> auth_req_id={auth_req_id} (guarded: pending approval)")

    # 3. Approve it as the authorized approver (brokered; admin key stays server-side).
    await _approval_gate(ticket)
    decision = await cp.approve_skill(auth_req_id, reason=f"broker approves {ticket}")
    print(f"  ✔ APPROVED {ticket} -> {decision.get('status')} by {decision.get('decided_by')}")

    # 4. Mint the skill-access token now that it's approved.
    token_body = await cp.mint_skill_token(auth_req_id)
    print(f"  minted skill-access token (scope={token_body.get('scope')}, "
          f"grant_id={token_body.get('grant_id')})")

    # 5. Grant it directly to the requesting agent over the queue.
    await cp.publish(deliver_to, {
        "ticket": ticket,
        "skill": skill, "action": action,
        "skill_access_token": token_body["access_token"],
        "scope": token_body.get("scope"),
        "grant_id": token_body.get("grant_id"),
        "granted_by": cp.agent_id,
    })
    print(f"  ✅ delivered grant for {ticket} to '{deliver_to}'")


async def main():
    async with ControlPlaneClient(CONTROL_PLANE) as cp:
        agent = await cp.register(
            name="DevOps Access Broker", kind="approver",
            agent_id=AGENT_ID,
            capabilities=["skill-approval", "access-escalation"],
            subscriptions=[REQUESTS_TOPIC],
            identity={"owner_principal": "rajarshic@gmail.com", "trust_level": "high",
                      "allowed_envs": ["dev"], "framework": "custom",
                      "target_application": "access-broker",
                      "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}]},
        )
        print(f"registered {agent['agent_id']} (idp_provisioned={agent['idp_provisioned']})")
        print(f"listening for escalation requests on '{REQUESTS_TOPIC}' …")
        # listen_resilient reconnects on WebSocket drops so the approver stays up
        # while idle waiting for requests (instead of exiting when the stream ends).
        async for msg in cp.listen_resilient([REQUESTS_TOPIC]):
            try:
                await handle_request(cp, msg["payload"])
            except Exception as exc:
                print(f"  ! error handling request: {exc}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
