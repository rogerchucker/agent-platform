# Demo — Agentic SRE incident response, with identity

This is the end-to-end story across three live services:

| Service | Role | Reachable (tailnet) |
|---|---|---|
| **Incy** | incident manager (PagerDuty-style) | `https://incy.tail353042.ts.net` |
| **Control plane** | agent registry, message queue, IdP broker, Incy bridge | `http://sre-control-plane` |
| **Agent IdP** | agent identities, grants, capabilities, tool gateway, CIBA skills, audit | `http://agent-idp` |

Agents talk **only** to the control plane; the IdP admin/internal keys never leave the server. Every "can this agent act?" check below is a **real** attestation / token exchange brokered to the IdP — not a mock.

The arc has two parts you can run independently.

---

## Part 1 — Incident response with human-in-the-loop approval

> A SEV0 fires in Incy. An autonomous SRE agent picks it up, but the runbook it
> needs is a **guarded** skill it can't self-grant. It escalates; a separate
> **access-broker** agent approves via **OpenID CIBA**; the responder then
> performs a **real guarded remediation** through the IdP tool gateway and
> resolves the incident. Incy's timeline reflects `unacked → investigating →
> closed`, driven entirely by the agent.

**Recommended for a live demo — one interactive flow, one terminal:**

```bash
cd agent-platform
CONTROL_PLANE=http://sre-control-plane python -u examples/devops/demo_interactive.py
```

This runs the whole incident as a single linear narration and **pauses for a real
`[y/N]` decision** at the approval moment — you, the on-call, approve or deny in
the same flow. `AUTO_APPROVE=y` (or `n`) runs it non-interactively for CI/smoke.

It **reuses the canonical agents** when they're already active (the always-on
`devops-access-broker`, and `devops-incident-responder`), registering them only
if missing, and never tears them down — so repeated runs don't spawn throwaways.
Both branches are real: approve → skill granted + guarded gateway remediation +
Incy resolved; deny → limited triage only.

**Fully autonomous (deployed).** Both the **access broker** and the **incident
responder** run as Deployments in the cluster, so incidents are handled with no
laptop process at all. They reconnect across WebSocket drops and **re-register if
the control plane restarts** (and use the `Recreate` strategy so a redeploy never
leaves two pods fighting over one agent identity). Deploy/update both with:

```bash
./examples/devops/k8s/deploy-agents.sh           # ConfigMaps (agent scripts) + Deployments
kubectl logs -n sre-control-plane deploy/incident-responder -f
kubectl logs -n sre-control-plane deploy/access-broker -f
```

Then just raise an incident and watch it resolve itself:

```bash
curl -X POST "http://sre-control-plane/incy/trigger?summary=Checkout%20down&severity=critical&dedup_key=demo-$(date +%s)"
```

**The agents are generic, not demo-specific.** Both are configured entirely by
env (the defaults below match this demo, but nothing is hardcoded to it):

| Responder env | default | meaning |
|---|---|---|
| `INCIDENT_SKILL` / `INCIDENT_SKILL_ACTION` | `pci-k8s-runbooks` / `use` | guarded skill it escalates for (empty → no escalation) |
| `SKILL_SUBJECT` | `system:serviceaccount:agent-requester:requester` | allow-listed subject for the skill |
| `REMEDIATION_TOOL` / `REMEDIATION_ACTION` / `REMEDIATION_RESOURCE` / `REMEDIATION_PARAMS` | `kubernetes` / `k8s.rollout.restart` / `kubernetes:cde/deploy/checkout` / `{}` | the guarded action run through the gateway |
| `RUNTIME_KIND` / `RUNTIME_CLUSTER` / `AGENT_ENV` | `cloud` / `local-dev` / `dev` | workload identity it attests with |

| Broker env | default | meaning |
|---|---|---|
| `BROKER_APPROVE_SKILLS` | `*` | approve allow-list (`*` = any) |
| `BROKER_DENY_SKILLS` | — | explicit deny-list |
| `BROKER_DENY` | — | `1` = deny everything |

> For the **interactive human-approval** demo (`demo_interactive.py`), scale the
> autonomous responder to 0 first so it doesn't grab the incident:
> `kubectl scale deploy/incident-responder -n sre-control-plane --replicas=0`
> (scale it back to 1 afterward).

**Two-agent variant** (responder + a separate access-broker process; output is
prefixed per agent):

```bash
./examples/devops/demo.sh                 # broker auto-approves after ~4s
BROKER_MANUAL=1 ./examples/devops/demo.sh  # press Enter in the broker to approve
```

What to point at as it runs:
- **`registered … (idp_provisioned=True)`** — registration provisioned a real IdP identity under the same id.
- **`[self-check] pci-k8s-runbooks:use -> BLOCKED`** — least privilege: the agent genuinely lacks the skill.
- **broker: `bc-authorize → pending → APPROVED`** — OpenID CIBA back-channel approval by the authorized approver.
- **`🔧 gateway executed k8s.rollout.restart … → executed`** — the remediation is a real capability minted + run through the IdP gateway (not a print).
- **Incy UI** flips `unacked → investigating → closed`, actor = the agent.

---

## Part 2 — Scale by cloning, decommission by deleting

> Under a surge you **clone** the responder to add capacity; when the surge
> passes you **delete** the clone — and its identity is genuinely revoked, so it
> can no longer act. This shows what clone/delete mean at the identity layer.

```bash
cd agent-platform
CONTROL_PLANE=http://sre-control-plane python -u examples/devops/clone_delete_demo.py
```

It runs five steps and prints ✓/✗ for each (exits non-zero on any failure, so it's also a smoke test):

1. **Register a base responder** → provisioned in the IdP, can attest & act.
2. **Clone as a replica** (`clone_bindings=true`) → fresh `agent_id`, **shares the workload identity**, can attest & act immediately (scale-out).
3. **Clone independent** (the default) → fresh id with **no runtime binding**, so it is **blocked from attesting until bound** — the safe default (a clone is a distinct identity, not a copy of credentials).
4. **Delete the replica** → the IdP **revokes its grants, deny-lists its in-flight tokens, and disables it**. Proof: the decommissioned agent now gets `403 agent_disabled` on attestation, and its identity reads `status: disabled`.
5. **The base agent is untouched** and still acts.

Sample tail:
```
STEP 4 DELETE the replica (decommission) — IdP teardown + revocation
    idp cascade: {'status': 'deleted', 'denied_jti': 2, 'revoked_grants': 0, 'revoked_skill_grants': 0}
    ✓ delete reported success
    ✓ decommissioned replica can NO LONGER act — HTTP 403 agent_disabled
    ✓ replica identity is disabled in the IdP — status=disabled
DEMO PASSED — clone (replica + independent) and delete-with-revocation all verified live.
```

---

## Reset between runs

Clear the dashboard's message feed and wipe Incy's incidents (keeps seed
services/users/integrations) for a clean slate:

```bash
./examples/devops/flush-demo.sh
```

It calls `DELETE /messages` on the control plane (live agent subscriptions are
untouched) and truncates Incy's event→alert→incident pipeline + timeline.

## Talking points (the "why it matters")

- **Agents are first-class identities.** Each agent attests its workload (k8s SA / SPIFFE / cloud) → short-lived token → scoped capability → audited gateway action. No standing credentials.
- **Least privilege + human approval.** Guarded skills require explicit approval (OpenID CIBA), brokered by an authorized approver — not self-granted.
- **Clone ≠ copy credentials.** A clone is a *new* identity. The default is independent (must bind its own runtime); replicas explicitly opt into sharing a workload identity for scale-out.
- **Delete actually revokes.** Decommissioning tears down grants, deny-lists outstanding tokens, and disables attestation — the agent can't act the moment it's deleted, provable in one call.
- **Everything is audited** in both the IdP audit log and the Incy incident timeline.
