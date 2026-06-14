"""Demo — cloning and deleting agents, and what it means for their identity.

Runs against a live control plane (which brokers the Agent IdP). It tells the
operator story behind the new lifecycle features:

  1. Register a base incident-responder      → gets an IdP identity, can act.
  2. CLONE as a replica (scale-out)           → fresh id, shares the workload
                                                 identity, can attest & act now.
  3. CLONE independent (the safe default)      → fresh id, NO runtime binding, so
                                                 it cannot attest until bound.
  4. DELETE the replica (decommission)         → IdP revokes its grants/tokens and
                                                 disables it; it can no longer act.
  5. The base agent is untouched and still acts.

Every "can it act?" check is a real attestation/token-exchange brokered to the
IdP — not a mock. Exits non-zero if any invariant fails, so it doubles as a
smoke test of the deployed clone/delete cascade.

    CONTROL_PLANE=http://sre-control-plane python -u examples/devops/clone_delete_demo.py
"""
import asyncio
import os
import sys
from pathlib import Path

# Run from anywhere: add the repo root (…/agent-platform) to the import path.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from control_plane.client import ControlPlaneClient

CP = os.environ.get("CONTROL_PLANE", "http://sre-control-plane")
RUNTIME = {"kind": "cloud", "cluster": "local-dev"}

# Clone SOURCE: reuse the canonical responder if it's running (don't spawn a
# throwaway), falling back to a temp agent only when running standalone.
CANONICAL_BASE = os.environ.get("BASE_AGENT", "devops-incident-responder")
FALLBACK_BASE = "demo-responder"
# The two demonstration clones (inherent to a clone demo) — always cleaned up.
REPLICA = "demo-responder-surge"
FORK = "demo-responder-fork"

DIM = "\033[2m"; G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; C = "\033[36m"; X = "\033[0m"
failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    mark = f"{G}✓{X}" if ok else f"{R}✗{X}"
    print(f"    {mark} {label}{(' — ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)


def step(n: int, title: str) -> None:
    print(f"\n{C}STEP {n}{X} {title}")


async def main() -> int:
    async with ControlPlaneClient(CP) as op:
        print(f"{DIM}control plane: {CP}{X}")
        # Clean up only the demonstration clones from a prior run (NOT any base).
        for aid in (REPLICA, FORK):
            try:
                await op.delete_agent(aid, hard=True)
            except Exception:
                pass

        step(1, "Pick the clone source (reuse the canonical responder if present)")
        created_base = False
        r = await op._http.get(f"/agents/{CANONICAL_BASE}")
        if r.status_code == 200:
            base = CANONICAL_BASE
            print(f"    reusing existing agent {C}{base}{X} as the clone source (won't be deleted)")
        else:
            base = FALLBACK_BASE
            created_base = True
            await op.register(
                name="Demo Responder", kind="incident-responder", agent_id=base,
                capabilities=["triage", "runbook-exec"], subscriptions=["incidents"],
                identity={"owner_principal": "rajarshic@gmail.com", "trust_level": "low",
                          "allowed_envs": ["dev"], "framework": "custom",
                          "target_application": "incident-response",
                          "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}]},
                auto_heartbeat=False)
            print(f"    no '{CANONICAL_BASE}' found — created temp base {C}{base}{X} (standalone mode)")
        st, _ = await op.try_attest(base, RUNTIME)
        check("base can attest & act", st == 200, f"token exchange → HTTP {st}")

        step(2, "CLONE as a replica (scale-out) — shares the workload identity")
        rep = await op.clone_agent(base, new_agent_id=REPLICA, clone_bindings=True,
                                   name="Surge Responder")
        check("replica is a fresh, provisioned identity",
              rep["agent_id"] == REPLICA and rep.get("idp_provisioned") is True)
        st, _ = await op.try_attest(REPLICA, RUNTIME)
        check("replica can attest & act immediately", st == 200, f"token exchange → HTTP {st}")

        step(3, "CLONE independent (the default) — no binding, must be bound first")
        fork = await op.clone_agent(base, new_agent_id=FORK)  # clone_bindings=False
        ident = await op.get_identity(FORK)
        check("independent clone has NO runtime bindings",
              ident.get("runtime_bindings") == [], f"bindings={ident.get('runtime_bindings')}")
        st, body = await op.try_attest(FORK, RUNTIME)
        check("independent clone is blocked until bound", st == 403,
              f"HTTP {st} {body.get('detail')}")

        step(4, "DELETE the replica (decommission) — IdP teardown + revocation")
        res = await op.delete_agent(REPLICA)
        print(f"    {DIM}idp cascade: {res}{X}")
        check("delete reported success",
              res.get("ok") is True and res.get("idp", {}).get("status") == "deleted")
        st, body = await op.try_attest(REPLICA, RUNTIME)
        check("decommissioned replica can NO LONGER act", st == 403,
              f"HTTP {st} {body.get('detail')}")
        ident = await op.get_identity(REPLICA)
        check("replica identity is disabled in the IdP",
              ident.get("status") == "disabled", f"status={ident.get('status')}")

        step(5, "The base agent is untouched and still acts")
        st, _ = await op.try_attest(base, RUNTIME)
        check("base still attests & acts", st == 200, f"token exchange → HTTP {st}")

        # Cleanup — hard-delete only the demonstration clones (REPLICA was only
        # soft-deleted in step 4). The base is deleted ONLY if we created it; the
        # canonical responder is left running untouched.
        for aid in (REPLICA, FORK):
            try:
                await op.delete_agent(aid, hard=True)
            except Exception:
                pass
        if created_base:
            try:
                await op.delete_agent(base, hard=True)
            except Exception:
                pass

    print()
    if failures:
        print(f"{R}DEMO FAILED{X}: {len(failures)} check(s) failed: {failures}")
        return 1
    print(f"{G}DEMO PASSED{X} — clone (replica + independent) and delete-with-revocation all verified live.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
