import argparse
import json
import os
import uuid
from typing import Any, Dict, List, Tuple
from urllib import error, request

import snowflake.connector
from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def _to_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _template_output(rec: Dict[str, Any], evidence: Dict[str, Any], supplemental: Dict[str, Any]) -> Tuple[str, str, List[Dict[str, str]], str, float]:
    rec_type = rec["RECOMMENDATION_TYPE"]
    object_name = rec["OBJECT_NAME"]
    credits = _to_float(evidence.get("credits") or evidence.get("credits_used"))
    spill = _to_float(evidence.get("spill_gb"))
    queue = _to_float(evidence.get("queue_seconds"))
    scanned = _to_float(evidence.get("scanned_gb"))
    confidence = 0.65
    if evidence.get("top_query_ids"):
        confidence += 0.10
    if credits > 0:
        confidence += 0.10

    summary = f"{rec_type} detected on {object_name} with evidence-backed cost and performance pressure."
    root = "Evidence indicates avoidable warehouse spend from workload shape and execution pattern."
    if rec_type == "MEMORY_PRESSURE":
        root = f"Remote/local spill reached {spill:.2f} GB, which suggests joins/aggregations exceed memory for current warehouse sizing."
    elif rec_type == "BAD_PRUNING":
        root = f"High scan volume ({scanned:.2f} GB) and weak partition pruning indicate broad table scans."
    elif rec_type == "CONGESTION":
        root = f"Queue time reached {queue:.2f} seconds, pointing to concurrency contention on this warehouse."
    elif rec_type == "SQL_COMPLEXITY":
        root = "Compilation overhead is high relative to elapsed time, often caused by complex dynamic SQL patterns."
    elif rec_type == "IDLE_BURN":
        root = "Warehouse credits are consumed while average running load remains low, indicating idle burn."
    elif rec_type == "INGESTION_WASTE":
        root = "Pipe failures and/or tiny file patterns increase ingest overhead and retries."

    fixes = [
        {
            "option": "quick_win",
            "action": "Apply the lowest-risk recommendation first and validate the next run.",
            "expected_impact": "Small to medium immediate credit reduction",
        },
        {
            "option": "best_fix",
            "action": "Tune top offending SQL and data layout using the listed query_ids and tables.",
            "expected_impact": "Largest sustained reduction in credits and runtime",
        },
        {
            "option": "long_term",
            "action": "Enforce query tag standards and budget guardrails for recurring jobs.",
            "expected_impact": "Prevents recurrence and improves attribution",
        },
    ]
    verification = (
        "Next run validation: compare credits, spill_gb, scanned_gb, queue_seconds and elapsed_seconds "
        "for the same query_tag/warehouse window. Confirm trend improves before broad rollout."
    )
    return summary, root, fixes, verification, min(confidence, 0.95)


def _call_external_llm(prompt: str) -> str:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    model = os.environ.get("OPENROUTER_MODEL", "openai/gpt-5.2")
    base_url = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
    site_url = os.environ.get("OPENROUTER_SITE_URL")
    site_name = os.environ.get("OPENROUTER_SITE_NAME")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a Snowflake cost optimization assistant. Use only supplied evidence. Never invent numbers."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if site_url:
        headers["HTTP-Referer"] = site_url
    if site_name:
        headers["X-Title"] = site_name
    req = request.Request(
        url=f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
    except error.URLError as exc:
        raise RuntimeError(f"LLM request failed: {exc}") from exc


def generate_ai_recommendations(limit: int = 100) -> int:
    conn = get_conn()
    created = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(
                """
                SELECT r.*
                FROM COST_COPILOT.RECOMMENDATIONS r
                LEFT JOIN COST_COPILOT.AI_RECOMMENDATIONS a ON r.rec_id = a.rec_id
                WHERE a.rec_id IS NULL
                ORDER BY r.created_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            recs = cur.fetchall()

            for rec in recs:
                evidence = rec["EVIDENCE_JSON"] or {}
                if isinstance(evidence, str):
                    evidence = json.loads(evidence)
                query_ids = evidence.get("top_query_ids", [])[:10]
                supplemental = {"queries": [], "warehouse_stats": []}
                if query_ids:
                    escaped_ids = []
                    for q in query_ids:
                        escaped_ids.append("'" + str(q).replace("'", "''") + "'")
                    safe_query_ids = ",".join(escaped_ids)
                    cur.execute(
                        f"""
                        SELECT query_id, query_tag, warehouse_name, start_time, total_elapsed_time, bytes_scanned,
                               bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage
                        FROM COST_COPILOT.FACT_QUERY
                        WHERE query_id IN ({safe_query_ids})
                        LIMIT 10
                        """
                    )
                    supplemental["queries"] = cur.fetchall()

                object_name = rec["OBJECT_NAME"]
                if rec["OBJECT_TYPE"] == "WAREHOUSE" and object_name:
                    cur.execute(
                        """
                        SELECT warehouse_name, start_time, credits_used, avg_running, avg_queued_load
                        FROM COST_COPILOT.FACT_WAREHOUSE_HOURLY
                        WHERE warehouse_name = %(warehouse)s
                          AND start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                        ORDER BY start_time DESC
                        LIMIT 24
                        """,
                        {"warehouse": object_name},
                    )
                    supplemental["warehouse_stats"] = cur.fetchall()

                ai_mode = "template"
                summary, root, fix_options, verification, confidence = _template_output(rec, evidence, supplemental)
                prompt = (
                    "Create grounded recommendations from this JSON only.\n"
                    "Return JSON with keys: ai_summary, ai_root_cause, ai_fix_options_json, ai_verification_steps, confidence_score.\n"
                    f"Recommendation: {json.dumps(rec, default=str)}\n"
                    f"Evidence: {json.dumps(evidence, default=str)}\n"
                    f"Supplemental: {json.dumps(supplemental, default=str)}"
                )
                if os.environ.get("OPENROUTER_API_KEY"):
                    try:
                        llm_text = _call_external_llm(prompt)
                        llm_json = json.loads(llm_text)
                        summary = llm_json.get("ai_summary", summary)
                        root = llm_json.get("ai_root_cause", root)
                        fix_options = llm_json.get("ai_fix_options_json", fix_options)
                        verification = llm_json.get("ai_verification_steps", verification)
                        confidence = _to_float(llm_json.get("confidence_score"), confidence)
                        ai_mode = "openrouter"
                    except Exception:
                        ai_mode = "template"

                cur.execute(
                    """
                    INSERT INTO COST_COPILOT.AI_RECOMMENDATIONS (
                      ai_rec_id, rec_id, ai_mode, ai_summary, ai_root_cause,
                      ai_fix_options_json, ai_verification_steps, confidence_score, evidence_snapshot
                    )
                    SELECT
                      %(ai_rec_id)s, %(rec_id)s, %(ai_mode)s, %(ai_summary)s, %(ai_root_cause)s,
                      PARSE_JSON(%(ai_fix_options_json)s), %(ai_verification_steps)s, %(confidence_score)s,
                      PARSE_JSON(%(evidence_snapshot)s)
                    """,
                    {
                        "ai_rec_id": str(uuid.uuid4()),
                        "rec_id": rec["REC_ID"],
                        "ai_mode": ai_mode,
                        "ai_summary": summary,
                        "ai_root_cause": root,
                        "ai_fix_options_json": json.dumps(fix_options),
                        "ai_verification_steps": verification,
                        "confidence_score": confidence,
                        "evidence_snapshot": json.dumps(
                            {"rule_evidence": evidence, "supplemental": supplemental},
                            default=str,
                        ),
                    },
                )
                created += 1
            conn.commit()
    finally:
        conn.close()
    return created


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    count = generate_ai_recommendations(limit=args.limit)
    print(f"Generated {count} AI recommendations.")


if __name__ == "__main__":
    main()
