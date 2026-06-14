#!/usr/bin/env bash
# Reset the demo to a clean slate:
#   1) flush the control-plane message queue (clears the dashboard "recent messages"
#      feed; live agent subscriptions are left intact), and
#   2) flush Incy's incidents — truncates the event→alert→incident pipeline and its
#      timeline/notifications, while preserving seed data (services, users,
#      integrations, escalation policies, schedules, webhook subscriptions).
#
# Usage:  examples/devops/flush-demo.sh
# Env:    CONTROL_PLANE (default http://sre-control-plane)
#         INCY_NS       (default incy)   — needs kubectl access to that namespace
set -uo pipefail

CONTROL_PLANE="${CONTROL_PLANE:-http://sre-control-plane}"
INCY_NS="${INCY_NS:-incy}"
rc=0

echo "▶ Flushing control-plane message queue ($CONTROL_PLANE) …"
if out=$(curl -fsS -X DELETE "$CONTROL_PLANE/messages" 2>&1); then
  echo "  $out"
else
  echo "  ⚠ could not reach control plane: $out"; rc=1
fi

echo "▶ Flushing Incy incidents (namespace $INCY_NS) …"
if kubectl exec -n "$INCY_NS" deploy/incy-postgres -- \
     psql -U incy -d incy -v ON_ERROR_STOP=1 -q -c \
     "TRUNCATE agent_task_updates, audit_logs, notification_attempts, alerts, incidents, events RESTART IDENTITY CASCADE;" 2>/dev/null; then
  echo "  ✔ incidents + timeline flushed (incident numbers reset; seed data preserved)"
else
  echo "  ⚠ incy flush failed (is kubectl pointed at the cluster, and the incy-postgres pod up?)"; rc=1
fi

[ "$rc" -eq 0 ] && echo "✔ demo reset complete." || echo "✖ finished with warnings."
exit "$rc"
