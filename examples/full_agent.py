"""A full SRE agent using every platform feature via the SDK.

It registers (provisioning an IdP identity), listens for root-cause tasks on the
message queue, and for each task obtains an access token, requests a grant,
mints a scoped capability, and acts through the IdP gateway — then publishes
its finding back to the queue.

Run against the tailnet deployment:
    python examples/full_agent.py
Push it a task:
    curl -X POST http://sre-control-plane/messages -H 'content-type: application/json' \
      -d '{"topic":"tasks.rootcause","payload":{"incident":"INC-42","resource":"pods/checkout"}}'
"""
import asyncio

from control_plane.client import ControlPlaneClient

CONTROL_PLANE = "http://sre-control-plane"   # tailnet hostname
RUNTIME = {"kind": "cloud", "cluster": "local-dev"}  # where this dev agent runs


async def main():
    async with ControlPlaneClient(CONTROL_PLANE) as cp:
        agent = await cp.register(
            name="rootcause-db", kind="rootcause",
            capabilities=["db-rootcause", "k8s-triage"],
            subscriptions=["tasks.rootcause"],
            identity={
                "owner_principal": "rajarshic@gmail.com",
                "trust_level": "low", "allowed_envs": ["dev"],
                "framework": "custom", "target_application": "sre-rootcause",
                "runtime_bindings": [RUNTIME],
            },
        )
        print(f"registered {agent['agent_id']} — idp_provisioned={agent['idp_provisioned']}")
        print("listening on tasks.rootcause …")

        async for msg in cp.listen(["tasks.rootcause"]):
            task = msg["payload"]
            resource = task.get("resource", "pods/unknown")
            ticket = task.get("incident", "INC-0")
            print(f"  task: {task}")

            # Identity-backed action: attestation → grant → capability → execute.
            access = await cp.get_access_token(runtime=RUNTIME, env="dev")
            grant = await cp.request_grant(
                action="k8s.get", resource=resource,
                purpose="rootcause", reason="investigate incident", ticket=ticket)
            cap = await cp.mint_capability(
                access, grant["grant_id"], "k8s.get", resource,
                purpose="rootcause", reason="investigate incident", ticket=ticket)
            result = await cp.execute(
                cap["capability_token"], tool="kubernetes",
                action="k8s.get", resource=resource, params={"namespace": "default"})
            print(f"  gateway executed: {result['status']}")

            await cp.publish("results.rootcause", {
                "incident": ticket, "resource": resource,
                "verdict": "OOMKilled — raise memory limit", "via": result["status"],
            })
            print("  posted result to results.rootcause")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
