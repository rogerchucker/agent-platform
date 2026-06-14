# DevOps agents — incy incident lifecycle + access escalation via CIBA

Two agents plus the control plane's **incy connector** demonstrate a full
incident response loop where incy's UI reflects the incident status the whole
way, and a guarded runbook skill is unblocked mid-incident over OpenID CIBA.

```
  incy ──(connector polls)──▶  incidents topic  ◀──(status writes)── Agent 1 (SRE)
   ▲                                  │                                   │
   │  acknowledge / resolve           ▼                                   │ needs guarded
   └──(connector applies)──   unacked→investigating→closed                │ runbook skill
                                                                          ▼
                              access.requests  ──▶  Agent 2 (access broker)
                                                     • find agent in registry
                                                     • bc-authorize → approve → mint (CIBA)
                              access.grants.* ◀──────  deliver skill token
```

## Incident states (incy ⇄ platform)

| platform status | incy native | when |
|-----------------|-------------|------|
| `unacked`       | `triggered`    | incy raises the incident |
| `investigating` | `acknowledged` | Agent 1 picks it up |
| `closed`        | `resolved`     | Agent 1 finishes |

The control plane's **incy connector** (`control_plane/incy.py`) bridges both
directions: it polls incy for new *triggered* incidents and publishes them onto
the `incidents` topic, and it consumes the agent's status writes off that same
topic and applies them to incy (`/acknowledge`, `/resolve`, + a timeline note),
recording the agent's incy user as the actor — so the UI shows it was **picked
up by the agent**.

## Run the demo

```bash
./examples/devops/demo.sh                  # broker reviews ~4s then approves
BROKER_MANUAL=1 ./examples/devops/demo.sh  # you press Enter to approve
```

It starts both agents, raises a real incy incident, and prints the incy status
transitions (`UNACKED → INVESTIGATING → CLOSED`) alongside the agents' logs.

Run pieces by hand:
```bash
export PYTHONPATH=$(pwd) PYTHONUNBUFFERED=1
python -u examples/devops/agent2_access_broker.py      # approver
python -u examples/devops/agent1_incident_responder.py # SRE agent
python    examples/devops/incy_alert.py "Checkout pods CrashLoopBackOff in CDE"
```

Watch it live in incy's web UI (the `incy` tailnet node) and on the platform
dashboard (`http://sre-control-plane`).

## What each agent exercises

**Agent 1 — `agent1_incident_responder.py`** (the SRE agent)
- first-time `register(... identity=...)`
- subscribes to the shared `incidents` topic
- on a new incy incident: **writes `investigating`** to that same topic (incy →
  acknowledged), investigates, then **writes `closed`** (incy → resolved)
- the investigation needs the *guarded* `pci-k8s-runbooks` skill it has no
  access to, so it escalates on `access.requests` and waits for the grant

**Agent 2 — `agent2_access_broker.py`** (the approver)
- subscribes to `access.requests` (separate from the incidents topic)
- finds the blocked agent via `list_agents()`, then runs the OpenID CIBA mint
  (`authorize_skill` → `approve_skill` → `mint_skill_token`) and delivers the
  skill token directly to the requester

## Config (control plane)

The connector is wired via env on the control-plane deployment:
`INCY_BASE_URL`, `INCY_INTEGRATION_KEY` (to raise demo incidents),
`INCY_AGENT_USER_ID` (the incy user recorded as the acting agent). See
`GET /incy/status`.
