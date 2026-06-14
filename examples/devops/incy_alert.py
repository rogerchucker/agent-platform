"""Raise a real incy incident (via the control plane's incy connector).

The control plane creates the incident in incy through its Events API; the
connector's poller then surfaces it onto the `incidents` topic for the SRE
agent to pick up.

    python examples/devops/incy_alert.py "Checkout pods CrashLoopBackOff in CDE"
"""
import sys
import time

import httpx

CONTROL_PLANE = "http://sre-control-plane"


def main():
    summary = sys.argv[1] if len(sys.argv) > 1 else "PCI cardholder-data pod failing readiness"
    r = httpx.post(f"{CONTROL_PLANE}/incy/trigger", params={
        "summary": summary, "severity": "critical", "dedup_key": f"demo-{int(time.time())}",
    }, timeout=10)
    r.raise_for_status()
    print(f"raised incy incident: {r.json().get('summary')} (event {r.json().get('id')})")


if __name__ == "__main__":
    main()
