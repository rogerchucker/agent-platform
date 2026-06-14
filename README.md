# SRE Agent Control Plane

The control plane for your SRE / RootCause agents. It handles three things:

1. **Registration** — agents announce themselves, their `kind`, and `capabilities`.
2. **Status** — each agent is **live** while it heartbeats (or holds a WebSocket);
   it flips to **inactive** automatically when it goes silent.
3. **Message queue** — a topic-based pub/sub bus that agents **listen** to and
   **post** to, over WebSocket (streaming) or REST.

A live web dashboard shows every agent's status and the recent message feed.

```
┌─────────────┐  register / heartbeat   ┌────────────────────┐
│  SRE agent  │ ───────────────────────▶│                    │
│ (rootcause) │ ◀── tasks (subscribe) ──│   Control Plane    │──▶ Dashboard
│             │ ──── results (publish) ─▶│  registry + queue  │
└─────────────┘                          └────────────────────┘
```

## Quick start

```bash
pip install -r requirements.txt
uvicorn control_plane.main:app --port 8080 --reload
```

Open **http://localhost:8080** for the live dashboard.

Run the demo agent in another terminal:

```bash
python examples/demo_agent.py
# then push it a task:
curl -X POST localhost:8080/messages -H 'content-type: application/json' \
  -d '{"topic":"tasks.rootcause","payload":{"incident":"INC-42","service":"checkout"}}'
```

## Agent SDK

One `ControlPlaneClient` covers every platform feature: registration (which
provisions an IdP identity), liveness, the message queue, and the full
identity/authorization chain brokered to the IdP.

```python
from control_plane.client import ControlPlaneClient

async with ControlPlaneClient("http://sre-control-plane") as cp:
    # Registration provisions a matching identity in the IdP (same agent_id).
    await cp.register(
        name="rootcause-db", kind="rootcause",
        capabilities=["db-rootcause"], subscriptions=["tasks.rootcause"],
        identity={"owner_principal": "you@example.com", "trust_level": "low",
                  "allowed_envs": ["dev"],
                  "runtime_bindings": [{"kind": "cloud", "cluster": "local-dev"}]},
    )
    # auto-heartbeats in the background; status stays "live"

    async for msg in cp.listen(["tasks.rootcause"]):     # streams tasks off the queue
        # Identity-backed action: attestation → grant → capability → gateway.
        rt = {"kind": "cloud", "cluster": "local-dev"}
        access = await cp.get_access_token(runtime=rt)
        grant = await cp.request_grant(action="k8s.get", resource="pods/checkout",
                                       purpose="rootcause", reason="crashloop", ticket="INC-42")
        cap = await cp.mint_capability(access, grant["grant_id"], "k8s.get", "pods/checkout",
                                       purpose="rootcause", reason="crashloop", ticket="INC-42")
        await cp.execute(cap["capability_token"], tool="kubernetes",
                         action="k8s.get", resource="pods/checkout")
        await cp.publish("results.rootcause", {"verdict": "OOMKilled"})
```

See `examples/full_agent.py` for a complete worker, and
`examples/devops/` for two agents that demonstrate a full **incident lifecycle**
(incy `unacked → investigating → closed`, driven by the SRE agent over the
`incidents` topic) plus **queue-driven access escalation via OpenID CIBA**: a
blocked agent asks for a skill on the message queue, and an approver agent finds
it in the registry and mints + grants the token over the bus.

### incy connector

When configured (`INCY_BASE_URL`, `INCY_INTEGRATION_KEY`, `INCY_AGENT_USER_ID`),
the control plane bridges the [incy](https://) incident system to the message
queue (`control_plane/incy.py`):

- **incy → queue**: polls incy for newly *triggered* incidents and publishes
  them onto the shared `incidents` topic (status `unacked`).
- **queue → incy**: consumes agent status writes off that topic and applies them
  to incy so its UI reflects them — `investigating` → acknowledge, `closed` →
  resolve, recording the agent's incy user as the actor (a timeline note shows
  it was picked up by the agent).

`GET /incy/status`, `GET /incy/incidents[/{id}]`, and `POST /incy/trigger`
(demo) expose the connector. State mapping: `unacked`=triggered,
`investigating`=acknowledged, `closed`=resolved.

### Identity & the IdP (trusted-broker model)

Registration provisions an identity in the running **Agent IdP**, and the control
plane brokers every identity call — agents never hold the IdP admin/internal
keys. The control plane is configured via `IDP_BASE_URL`, `IDP_ADMIN_API_KEY`,
`IDP_INTERNAL_API_KEY` (a k8s `Secret`); if unset, identity provisioning is
skipped and the queue/registry still work.

SDK identity surface → brokered IdP endpoints:

| SDK method | Flow |
|------------|------|
| `register(..., identity=…)` | provisions `POST /agents` in the IdP |
| `get_identity()` | the agent's IdP record |
| `get_access_token(runtime, env)` | attestation → agent access token |
| `request_grant(action, resource, …)` | creates a grant |
| `mint_capability(token, grant_id, …)` | mints a scoped, short-lived capability |
| `execute(cap_token, tool, action, resource)` | acts through the IdP gateway |
| `request_skill_access(skill_id, action)` | skill access via OIDC CIBA (auto/human approval) |
| `authorize_skill` / `approve_skill` / `mint_skill_token` | CIBA building blocks for an approver agent |
| `list_agents()` | find peer agents in the registry |
| `introspect(token)` | RFC 7662-style token introspection |

Broker endpoints live under `/agents/{id}/identity/*` and `/idp/*`
(`GET /idp/status` reports whether the IdP is wired up). `/idp/skills/approve`
lets a registered approver agent decide a pending CIBA request — the control
plane verifies the approver is registered before forwarding to the IdP.

## API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/agents` | Register (idempotent if you pass your own `agent_id`) |
| `DELETE` | `/agents/{id}` | Deregister |
| `POST` | `/agents/{id}/heartbeat` | Keep-alive → status `live` |
| `GET` | `/agents` | List agents + status |
| `POST` | `/messages` | Publish `{topic, payload}` to the queue |
| `GET` | `/messages?topic=&limit=` | Recent message history |
| `GET` | `/topics` | Known topics + subscriber count |
| `WS` | `/ws/{id}?topics=a,b` | Full-duplex listen + post |
| `GET` | `/healthz` | Liveness probe |

### WebSocket frames (client → server)

```json
{"type": "publish",   "topic": "results.triage", "payload": {...}}
{"type": "subscribe", "topics": ["tasks.*", "incidents"]}
{"type": "heartbeat"}
```

Server → client frames: `ready`, `message`, `publish_ack`, `heartbeat_ack`, `error`.

## Topics & wildcards

Topics are dot-namespaced. A subscription supports a single trailing wildcard:
`tasks` or `tasks.*` both match `tasks.rootcause`, `tasks.triage`, …; `*` matches
everything. Useful conventions: `tasks.<kind>`, `results.<kind>`, `incidents`.

## Liveness model

- Heartbeat (REST or WS) or an open WebSocket → **live**.
- No heartbeat for `HEARTBEAT_TIMEOUT` (default 30s) → **inactive**, set by a
  background sweeper so the dashboard is correct even if the agent never calls in.
- `DELETE /agents/{id}` → **deregistered** (hidden by default; can't be revived
  by a heartbeat — must re-register).

## Tests

```bash
python -m pytest tests/ -q
```

## Deploy to Kubernetes (DOKS)

The platform is containerized and runs on the `do-nyc1-simple-dev-cluster` DOKS
cluster in namespace `sre-control-plane`, exposed via a DigitalOcean
LoadBalancer.

```bash
./k8s/deploy.sh            # build linux/amd64, push to DOCR, apply, wait for LB
# or manually:
kubectl apply -f k8s/manifests.yaml
kubectl -n sre-control-plane get svc control-plane   # read EXTERNAL-IP
```

Open the **EXTERNAL-IP** in a browser for the live portal. It shows which agents
are live, the message-queue feed (what agents are saying), and cluster findings
agents publish (topic `cluster.findings`).

### Access over Tailscale (for local agent development)

A userspace Tailscale sidecar (`k8s/tailscale.yaml`) joins the control plane to
the tailnet as node **`sre-control-plane`**, so you can build and validate agents
from your laptop without exposing anything publicly:

```bash
kubectl apply -f k8s/tailscale.yaml
# authorize the new node — either visit the login URL in the pod logs:
kubectl -n sre-control-plane logs deploy/sre-control-plane-ts-proxy | grep login.tailscale
# or supply an auth key for non-interactive joins:
kubectl -n sre-control-plane create secret generic tailscale-authkey \
  --from-literal=authkey=tskey-auth-xxxxx
```

Then point your agent SDK at the tailnet hostname — REST **and** the WebSocket
message queue both work:

```python
async with ControlPlaneClient("http://sre-control-plane") as cp:
    await cp.register(name="my-agent", kind="rootcause", subscriptions=["tasks.rootcause"])
    async for msg in cp.listen(["tasks.rootcause"]):
        await cp.publish("cluster.findings", {...})
```

It forwards tailnet port 80 → the in-cluster service via `tailscale serve`
(`TCP.80.TCPForward`), mirroring the `agent-idp`/`incy` `*-ts-proxy` pattern.

Manifests (`k8s/manifests.yaml`): `Namespace`, single-replica `Deployment` (in-
memory state), and a `LoadBalancer` `Service` on port 80 → 8080. Notes:
- The DOCR plan allows only one repository, so the image is pushed as a
  namespaced **tag** in the existing `agent-idp` repo:
  `agent-idp:sre-control-plane-<version>`.
- Nodes are amd64; the image must be built `--platform linux/amd64`.

## Notes / next steps

State is in-memory (single-process), which is the right default for a control
plane you run as one service. To scale horizontally, back the registry and queue
with Redis (pub/sub + hashes) or NATS — the `AgentRegistry` and `MessageQueue`
classes are the seams to swap. Auth (per-agent tokens) and persistence of the
message history are the natural next additions.
