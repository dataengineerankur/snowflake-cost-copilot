import argparse
import json
import os
from urllib import request

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def _post_slack(message: str):
    webhook = os.environ.get("ALERT_SLACK_WEBHOOK")
    if not webhook:
        return False
    payload = {"text": message}
    req = request.Request(
        webhook,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=10):
        return True


def _send_email_stub(message: str):
    email_target = os.environ.get("ALERT_EMAIL_TO")
    if not email_target:
        return False
    print(f"[EMAIL-STUB] To={email_target}: {message}")
    return True


def dispatch_open_anomalies(limit: int = 20) -> int:
    conn = get_conn()
    sent = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT anomaly_id, object_name, severity, anomaly_score, observed_value, baseline_value
                FROM COST_COPILOT.ANOMALIES
                WHERE status = 'OPEN'
                ORDER BY detected_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            for anomaly_id, object_name, severity, score, observed, baseline in cur.fetchall():
                msg = (
                    f"[CostCopilot] {severity} anomaly on {object_name}: score={float(score or 0):.2f}, "
                    f"observed={float(observed or 0):.4f}, baseline={float(baseline or 0):.4f}"
                )
                ok = _post_slack(msg) or _send_email_stub(msg)
                if ok:
                    sent += 1
    finally:
        conn.close()
    return sent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    sent = dispatch_open_anomalies(limit=args.limit)
    print(f"Sent {sent} anomaly alerts.")


if __name__ == "__main__":
    main()
