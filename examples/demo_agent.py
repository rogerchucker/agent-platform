"""A demo SRE agent: registers, listens for tasks, posts results.

Run the control plane first:
    uvicorn control_plane.main:app --port 8080
Then:
    python examples/demo_agent.py
"""
import asyncio

from control_plane.client import ControlPlaneClient


async def main():
    async with ControlPlaneClient("http://localhost:8080") as cp:
        agent = await cp.register(
            name="rootcause-db",
            kind="rootcause",
            capabilities=["db-rootcause", "slow-query-analysis"],
            subscriptions=["tasks.rootcause"],
        )
        print(f"registered as {agent['agent_id']} (status={agent['status']})")
        print("listening on tasks.rootcause … (Ctrl-C to quit)")

        async for msg in cp.listen(["tasks.rootcause"]):
            task = msg["payload"]
            print(f"  got task: {task}")
            # … do real root-cause analysis here …
            await cp.publish("results.rootcause", {
                "task": task, "verdict": "connection pool exhausted", "confidence": 0.82,
            })
            print("  posted result to results.rootcause")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
