#!/usr/bin/env bash
# Live demo: incy incident → SRE agent picks it up & drives its status through
# the message queue, while incy's UI reflects unacked → investigating → closed.
# The access-broker also unblocks the agent's runbook skill via OpenID CIBA.
#
#   ./examples/devops/demo.sh                  # broker reviews ~4s, then approves
#   BROKER_MANUAL=1 ./examples/devops/demo.sh  # you press Enter to approve
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export PYTHONPATH="$ROOT" PYTHONUNBUFFERED=1
export BROKER_APPROVAL_DELAY="${BROKER_APPROVAL_DELAY:-4}"
export BROKER_MANUAL="${BROKER_MANUAL:-0}"

BROKER=$'\033[36m[BROKER ]\033[0m'
A1=$'\033[33m[AGENT-1]\033[0m'
SYS=$'\033[2m[ demo  ]\033[0m'
INCY=$'\033[35m[ incy  ]\033[0m'

prefix() { local tag="$1"; while IFS= read -r line; do printf '%s %s\n' "$tag" "$line"; done; }

pids=()
cleanup() { echo; printf '%s stopping agents…\n' "$SYS"; kill "${pids[@]}" 2>/dev/null; }
trap cleanup EXIT INT TERM

printf '%s starting access-broker (approver)\n' "$SYS"
python3 -u "$ROOT/examples/devops/agent2_access_broker.py" 2>&1 | prefix "$BROKER" &
pids+=($!)
sleep 5

printf '%s starting incident-responder (SRE agent)\n' "$SYS"
python3 -u "$ROOT/examples/devops/agent1_incident_responder.py" 2>&1 | prefix "$A1" &
pids+=($!)
sleep 6

printf '%s raising a NEW incy incident…\n\n' "$SYS"
# Trigger the incident and watch incy's status reflect the agent's progress.
SUMMARY="Checkout pods CrashLoopBackOff in CDE" python3 - "$INCY" <<'PY'
import os, sys, time, httpx
INCY = sys.argv[1]
CP = "http://sre-control-plane"
summary = os.environ["SUMMARY"]
c = httpx.Client(base_url=CP, timeout=10)
c.post("/incy/trigger", params={"summary": summary, "severity": "critical",
                                "dedup_key": f"demo-{int(time.time())}"}).raise_for_status()
print(f"{INCY} created '{summary}' (status: triggered = UNACKED)")

# find our incident, then watch its status change as the agent drives it
iid, last = None, None
deadline = time.time() + 40
while time.time() < deadline:
    incs = c.get("/incy/incidents", params={"limit": 30}).json()["incidents"]
    if iid is None:
        m = [i for i in incs if i["title"] == summary]
        if m: iid = m[0]["id"]
    if iid:
        inc = c.get(f"/incy/incidents/{iid}").json()
        st = inc["status"]
        if st != last:
            label = {"triggered": "UNACKED", "acknowledged": "INVESTIGATING", "resolved": "CLOSED"}.get(st, st)
            who = inc.get("acknowledged_by") or inc.get("resolved_by") or "-"
            print(f"{INCY} incident status → {st}  ({label})   actor={who}")
            last = st
        if st == "resolved":
            print(f"{INCY} ✅ incy UI now shows this incident CLOSED, picked up & resolved by the agent")
            break
    time.sleep(2)
else:
    print(f"{INCY} (stopped watching; last status={last})")
PY

printf '\n%s demo complete.\n' "$SYS"
