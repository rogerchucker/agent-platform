#!/usr/bin/env bash
# Deploy / update the always-on access broker in the cluster. Injects the broker
# script (examples/devops/agent2_access_broker.py) as a ConfigMap, then applies
# the Deployment and restarts it to pick up the latest script.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
NS=sre-control-plane

kubectl create configmap access-broker-src -n "$NS" \
  --from-file=broker_agent.py="$ROOT/examples/devops/agent2_access_broker.py" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f "$HERE/access-broker.yaml"
kubectl rollout restart deploy/access-broker -n "$NS"
kubectl rollout status deploy/access-broker -n "$NS" --timeout=120s
echo "access-broker is live:"
kubectl get pods -n "$NS" -l app=access-broker
