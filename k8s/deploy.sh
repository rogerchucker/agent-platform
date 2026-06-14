#!/usr/bin/env bash
# Build, push, and deploy the SRE control plane to the DOKS cluster.
#
# Notes for this environment:
#  - The DOCR plan (raj-agent-idp) allows only ONE repository, already used by
#    "agent-idp". So we push a namespaced TAG into that repo rather than a new
#    repo: agent-idp:sre-control-plane-<version>.
#  - DOKS nodes are amd64; the build host is Apple Silicon -> build linux/amd64.
set -euo pipefail

REGISTRY=registry.digitalocean.com/raj-agent-idp
REPO=agent-idp
VERSION=${1:-0.1.0}
IMG="$REGISTRY/$REPO:sre-control-plane-$VERSION"
NS=sre-control-plane
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo ">> logging in to DOCR"
doctl registry login

echo ">> building + pushing $IMG (linux/amd64)"
docker buildx build --platform linux/amd64 -t "$IMG" --push "$ROOT"

echo ">> ensuring namespace + registry pull secret"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
doctl registry kubernetes-manifest --namespace "$NS" | kubectl apply -f -

echo ">> applying manifests"
sed "s#image: .*#image: $IMG#" "$ROOT/k8s/manifests.yaml" | kubectl apply -f -

echo ">> waiting for rollout"
kubectl -n "$NS" rollout status deploy/control-plane --timeout=180s

echo ">> waiting for LoadBalancer IP"
for i in $(seq 1 30); do
  IP=$(kubectl -n "$NS" get svc control-plane -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
  [ -n "$IP" ] && break
  sleep 10
done
echo ">> portal available at: http://${IP:-<pending>}"
