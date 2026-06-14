#!/usr/bin/env bash
# Deploy / update the always-on demo agents in the cluster: the access broker
# (approver) AND the incident responder. Each runs as a Deployment that reuses
# the control-plane image with its agent script injected via a ConfigMap, so no
# extra image build is needed. Both reconnect on WS drops and re-register if the
# control plane restarts.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
NS=sre-control-plane

echo "▶ access broker"
kubectl create configmap access-broker-src -n "$NS" \
  --from-file=broker_agent.py="$ROOT/examples/devops/agent2_access_broker.py" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$HERE/access-broker.yaml"

echo "▶ incident responder"
kubectl create configmap incident-responder-src -n "$NS" \
  --from-file=responder_agent.py="$ROOT/examples/devops/agent1_incident_responder.py" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$HERE/incident-responder.yaml"

# Restart both to pick up the latest scripts, then wait.
kubectl rollout restart deploy/access-broker deploy/incident-responder -n "$NS"
kubectl rollout status deploy/access-broker -n "$NS" --timeout=120s
kubectl rollout status deploy/incident-responder -n "$NS" --timeout=120s
echo "agents live:"
kubectl get pods -n "$NS" -l 'app in (access-broker,incident-responder)'
