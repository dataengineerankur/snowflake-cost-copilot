import json
import os
import re
import subprocess
import sys
import uuid
from base64 import b64encode
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from time import time
from typing import Any, Dict, List, Optional

import snowflake.connector
from fastapi import FastAPI, Query, WebSocket
from pydantic import BaseModel
from urllib import error, request

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

from sf_bootstrap import ensure_copilot_bootstrap, get_conn

app = FastAPI(title="Snowflake Cost Copilot Chat API")
_BOOTSTRAP_DONE = False
_RETRIEVE_CACHE: Dict[str, Dict[str, Any]] = {}
_RETRIEVE_CACHE_TTL_SECONDS = 45
_DASHBOARD_CACHE: Dict[str, Dict[str, Any]] = {}
_DASHBOARD_CACHE_TTL_SECONDS = 90


class ChatRequest(BaseModel):
    question: str
    time_range: Optional[str] = "7d"
    history: Optional[List[Dict[str, Any]]] = None
    llm_mode: Optional[str] = None
    llm_provider: Optional[str] = None


class SimulationRequest(BaseModel):
    target_type: str = "WAREHOUSE"
    target_name: str
    scan_reduction_pct: float = 0.2
    spill_reduction_pct: float = 0.3
    warehouse_resize: bool = False


class ApprovalRequest(BaseModel):
    rec_id: str
    approved: bool
    approved_by: Optional[str] = None
    approval_note: Optional[str] = None


class LLMTestRequest(BaseModel):
    provider: str = "openrouter"
    api_key: Optional[str] = None
    model: Optional[str] = None
    base_url: Optional[str] = None


class ApplyRecommendationRequest(BaseModel):
    mode: str = "DRY_RUN"
    provider: str = "openrouter"
    api_key: Optional[str] = None
    model: Optional[str] = None
    base_url: Optional[str] = None
    auto_create_pr: bool = True
    base_branch: str = "main"


class RagReindexRequest(BaseModel):
    time_range: str = "30d"
    max_rows_per_source: int = 200


def _provider_from_inputs(llm_mode_override: Optional[str], provider_override: Optional[str] = None) -> Optional[str]:
    llm_mode = (llm_mode_override or os.environ.get("CHAT_USE_LLM", "auto")).lower().strip()
    if llm_mode == "off":
        return None

    provider = (provider_override or os.environ.get("LLM_PROVIDER", "openrouter")).lower().strip()
    if provider not in {"openrouter", "groq", "cursor"}:
        provider = "openrouter"
    return provider


def _resolve_chat_provider(requested_provider: Optional[str]) -> Dict[str, Any]:
    if not requested_provider:
        return {"provider": None, "reason": "LLM_MODE_OFF"}
    provider = (requested_provider or "openrouter").lower().strip()
    if provider == "cursor":
        # Cursor provider is supported for remediation agent/PR creation, not chat completions.
        if os.environ.get("OPENROUTER_API_KEY"):
            return {"provider": "openrouter", "reason": "CURSOR_CHAT_UNSUPPORTED_FALLBACK_OPENROUTER"}
        if os.environ.get("GROQ_API_KEY"):
            return {"provider": "groq", "reason": "CURSOR_CHAT_UNSUPPORTED_FALLBACK_GROQ"}
        return {"provider": None, "reason": "CURSOR_CHAT_UNSUPPORTED_NO_FALLBACK_KEY"}
    return {"provider": provider, "reason": "OK"}


def _provider_chain(chat_provider: Optional[str]) -> List[str]:
    if not chat_provider:
        return []
    chain = [chat_provider]
    if chat_provider == "openrouter" and os.environ.get("GROQ_API_KEY"):
        chain.append("groq")
    elif chat_provider == "groq" and os.environ.get("OPENROUTER_API_KEY"):
        chain.append("openrouter")
    return chain


def _call_with_failover(chain: List[str], messages: List[Dict[str, str]]) -> Dict[str, Any]:
    errors: List[str] = []
    for provider in chain:
        out = _call_chat_provider(provider, messages)
        if out:
            return {"ok": True, "provider": provider, "response": out, "errors": errors}
        errors.append(f"{provider}_failed")
    return {"ok": False, "provider": None, "response": None, "errors": errors}


def _provider_config(provider: str, api_key_override: Optional[str] = None, model_override: Optional[str] = None, base_url_override: Optional[str] = None):
    provider = provider.lower().strip()
    if provider == "groq":
        return {
            "provider": "groq",
            "api_key": api_key_override or os.environ.get("GROQ_API_KEY"),
            "model": model_override or os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b"),
            "base_url": (base_url_override or os.environ.get("GROQ_BASE_URL", "https://api.groq.com/openai/v1")).rstrip("/"),
            "headers": {},
        }
    if provider == "cursor":
        return {
            "provider": "cursor",
            "api_key": api_key_override or os.environ.get("CURSOR_API_KEY"),
            "model": model_override or os.environ.get("CURSOR_MODEL", "cursor-default"),
            "base_url": (base_url_override or os.environ.get("CURSOR_BASE_URL", "https://api.cursor.com")).rstrip("/"),
            "headers": {},
        }
    return {
        "provider": "openrouter",
        "api_key": api_key_override or os.environ.get("OPENROUTER_API_KEY"),
        "model": model_override or os.environ.get("OPENROUTER_MODEL", "openai/gpt-5.2"),
        "base_url": (base_url_override or os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")).rstrip("/"),
        "headers": {
            "HTTP-Referer": os.environ.get("OPENROUTER_SITE_URL", ""),
            "X-Title": os.environ.get("OPENROUTER_SITE_NAME", ""),
        },
    }


def _parse_llm_json(txt: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(txt)
        if isinstance(parsed, dict):
            return parsed
        return {"summary": str(parsed), "recommended_actions": [], "assumptions": []}
    except json.JSONDecodeError:
        start = txt.find("{")
        end = txt.rfind("}")
        if start != -1 and end != -1 and end > start:
            parsed = json.loads(txt[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        return {"summary": txt, "recommended_actions": [], "assumptions": []}


def _call_chat_provider(
    provider: str,
    messages: List[Dict[str, str]],
    *,
    api_key_override: Optional[str] = None,
    model_override: Optional[str] = None,
    base_url_override: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    cfg = _provider_config(provider, api_key_override, model_override, base_url_override)
    api_key = cfg["api_key"]
    if not api_key:
        return None
    if cfg["provider"] == "cursor":
        return None

    base_url = cfg["base_url"]
    model = cfg["model"]
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    for hk, hv in cfg.get("headers", {}).items():
        if hv:
            headers[hk] = hv

    payload = {"model": model, "messages": messages, "temperature": 0.1, "max_tokens": 1800}
    req = request.Request(
        f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=18) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            txt = str(body["choices"][0]["message"]["content"])
            return _parse_llm_json(txt)
    except Exception:
        return None


def _json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _ensure_bootstrap_once(conn):
    global _BOOTSTRAP_DONE
    if not _BOOTSTRAP_DONE:
        ensure_copilot_bootstrap(conn)
        _BOOTSTRAP_DONE = True


def _time_filter_sql(time_range: str) -> str:
    if not time_range:
        return "DATEADD(DAY, -7, CURRENT_TIMESTAMP())"
    t = time_range.lower().strip()
    if t in ("24h", "last_24h", "last24h", "1d"):
        return "DATEADD(HOUR, -24, CURRENT_TIMESTAMP())"
    if t in ("7d", "last_7d", "last7d"):
        return "DATEADD(DAY, -7, CURRENT_TIMESTAMP())"
    if t in ("30d", "last_30d", "last30d"):
        return "DATEADD(DAY, -30, CURRENT_TIMESTAMP())"
    return "DATEADD(DAY, -7, CURRENT_TIMESTAMP())"


def _classify_intent(question: str, history: Optional[List[Dict[str, Any]]] = None) -> str:
    q_original = question.lower().strip()
    q = q_original
    # If user asks a short follow-up, enrich with previous user prompt.
    if history and len(q_original.split()) <= 5:
        for msg in reversed(history):
            if msg.get("role") == "user" and msg.get("content"):
                q = f"{q} {msg['content'].lower()}"
                break
    # Always prioritize the current prompt intent first (avoid history flipping "worst" to "best").
    if any(x in q_original for x in ("worst", "least efficient", "slowest", "most expensive", "highest paid")) and ("job" in q_original or "query" in q_original):
        return "heavy_jobs"
    if "best" in q_original and ("job" in q_original or "query" in q_original):
        return "best_jobs"
    if "best" in q and ("job" in q or "query" in q):
        return "best_jobs"
    if ("highest paid" in q or "most expensive" in q or "expensive" in q) and ("job" in q or "query" in q):
        return "heavy_jobs"
    if "spill" in q:
        return "spill"
    if "pruning" in q or "scan" in q:
        return "pruning"
    if "idle" in q or "warehouse" in q:
        return "warehouse_idle"
    if "pipe" in q or "ingest" in q or "copy" in q:
        return "pipe"
    if "task" in q:
        return "tasks"
    if "spike" in q:
        return "cost_spike"
    if "recommend" in q:
        return "recommendations"
    return "heavy_jobs"


def _rows_to_dicts(cursor) -> List[Dict]:
    cols = [c[0] for c in cursor.description]
    return [_json_safe(dict(zip(cols, row))) for row in cursor.fetchall()]


def _run_query_dict(sql: str, params=None, bootstrap: bool = True):
    conn = get_conn()
    try:
        if bootstrap:
            _ensure_bootstrap_once(conn)
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, params or {})
            return [_json_safe({k.lower(): v for k, v in row.items()}) for row in cur.fetchall()]
    finally:
        conn.close()


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _rag_tokens(question: str) -> List[str]:
    raw = [t.strip(" ,.:;!?()[]{}\"'").lower() for t in question.split()]
    return [t for t in raw if len(t) >= 4][:8]


def _rag_fetch(question: str, limit: int = 8) -> List[Dict[str, Any]]:
    tokens = _rag_tokens(question)
    if not tokens:
        return []
    conn = get_conn()
    try:
        _ensure_bootstrap_once(conn)
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            token_expr = " + ".join(
                [f"IFF(CONTAINS(LOWER(chunk_text), %(tok{i})s), 1, 0)" for i in range(len(tokens))]
            )
            params = {f"tok{i}": tok for i, tok in enumerate(tokens)}
            params["limit"] = limit
            cur.execute(
                f"""
                SELECT chunk_id, source_type, source_name, chunk_text,
                       ({token_expr}) AS keyword_score,
                       created_at
                FROM COST_COPILOT.RAG_CHUNKS
                QUALIFY keyword_score > 0
                ORDER BY keyword_score DESC, created_at DESC
                LIMIT %(limit)s
                """,
                params,
            )
            return [{k.lower(): _json_safe(v) for k, v in row.items()} for row in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def _classify_intent_agentic(question: str, history: Optional[List[Dict[str, Any]]], chain: List[str]) -> Optional[str]:
    if not chain:
        return None
    payload = {
        "question": question,
        "history": history or [],
        "allowed_intents": ["best_jobs", "heavy_jobs", "spill", "pruning", "warehouse_idle", "pipe", "tasks", "cost_spike", "recommendations"],
    }
    messages = [
        {
            "role": "system",
            "content": (
                "Classify user request into one allowed intent. Return strict JSON: "
                '{"intent":"<allowed_intent>","reason":"short"}'
            ),
        },
        {"role": "user", "content": json.dumps(payload)},
    ]
    out = _call_with_failover(chain, messages)
    if not out.get("ok"):
        return None
    intent = (out["response"] or {}).get("intent")
    if isinstance(intent, str) and intent in payload["allowed_intents"]:
        return intent
    return None


def _guardrail_intent(question: str, proposed_intent: Optional[str]) -> Optional[str]:
    q = (question or "").lower()
    if any(x in q for x in ("worst", "most expensive", "highest paid", "slowest")) and ("job" in q or "query" in q):
        return "heavy_jobs"
    if "best" in q and ("job" in q or "query" in q):
        return "best_jobs"
    return proposed_intent


def _run_agentic_methodology(
    *,
    question: str,
    intent: str,
    rows: List[Dict[str, Any]],
    actions: List[Dict[str, Any]],
    history: Optional[List[Dict[str, Any]]],
    chain: List[str],
    rag_chunks: List[Dict[str, Any]],
    max_budget_s: float = 40.0,
) -> Dict[str, Any]:
    if not chain:
        return {"ok": False, "reason": "NO_PROVIDER"}
    t0 = time()

    context = {
        "intent": intent,
        "question": question,
        "rows": rows[:10],
        "actions": actions[:8],
        "history": history or [],
        "rag": rag_chunks[:8],
    }

    planner_messages = [
        {"role": "system", "content": 'Plan analysis. Return strict JSON: {"plan":[...],"focus_metrics":[...],"intent_confirmed":"..."}'},
        {"role": "user", "content": json.dumps(context)},
    ]
    if (time() - t0) > max_budget_s:
        return {"ok": False, "reason": "TIME_BUDGET_EXCEEDED_PLANNER"}
    planner = _call_with_failover(chain, planner_messages)
    if not planner.get("ok"):
        return {"ok": False, "reason": "PLANNER_FAILED", "errors": planner.get("errors", [])}

    analyst_messages = [
        {"role": "system", "content": 'Analyze evidence and RAG context. Return strict JSON: {"findings":[...],"risk_level":"LOW|MEDIUM|HIGH","actions":[...],"plain_english":"..."}'},
        {"role": "user", "content": json.dumps({"context": context, "plan": planner["response"]})},
    ]
    if (time() - t0) > max_budget_s:
        return {"ok": False, "reason": "TIME_BUDGET_EXCEEDED_ANALYST"}
    analyst = _call_with_failover(chain, analyst_messages)
    if not analyst.get("ok"):
        return {"ok": False, "reason": "ANALYST_FAILED", "errors": analyst.get("errors", [])}

    responder_messages = [
        {
            "role": "system",
            "content": (
                'Compose final grounded answer. Return strict JSON keys: '
                "summary, recommended_actions, assumptions, follow_up_suggestions."
            ),
        },
        {"role": "user", "content": json.dumps({"context": context, "plan": planner["response"], "analysis": analyst["response"]})},
    ]
    if (time() - t0) > max_budget_s:
        return {"ok": False, "reason": "TIME_BUDGET_EXCEEDED_RESPONDER"}
    responder = _call_with_failover(chain, responder_messages)
    if not responder.get("ok"):
        return {"ok": False, "reason": "RESPONDER_FAILED", "errors": responder.get("errors", [])}

    return {
        "ok": True,
        "provider": responder.get("provider") or analyst.get("provider") or planner.get("provider"),
        "planner": planner.get("response", {}),
        "analysis": analyst.get("response", {}),
        "final": responder.get("response", {}),
    }


def _run_direct_llm_grounded(
    *,
    question: str,
    intent: str,
    rows: List[Dict[str, Any]],
    actions: List[Dict[str, Any]],
    history: Optional[List[Dict[str, Any]]],
    rag_chunks: List[Dict[str, Any]],
    chain: List[str],
) -> Dict[str, Any]:
    if not chain:
        return {"ok": False}
    payload = {
        "question": question,
        "intent": intent,
        "rows": rows[:5],
        "actions": actions[:5],
        "history": (history or [])[-6:],
        "rag": rag_chunks[:4],
    }
    messages = [
        {
            "role": "system",
            "content": (
                "You are a Snowflake FinOps copilot. Use only provided evidence and rag context. "
                "Return strict JSON keys: summary, recommended_actions, assumptions, follow_up_suggestions."
            ),
        },
        {"role": "user", "content": json.dumps(payload)},
    ]
    out = _call_with_failover(chain, messages)
    if not out.get("ok"):
        return {"ok": False, "errors": out.get("errors", [])}
    return {"ok": True, "provider": out.get("provider"), "final": out.get("response", {})}


def _run_minimal_llm_rescue(
    *,
    question: str,
    intent: str,
    rows: List[Dict[str, Any]],
    chain: List[str],
) -> Dict[str, Any]:
    if not chain:
        return {"ok": False}
    payload = {
        "question": question,
        "intent": intent,
        "rows": rows[:3],
        "constraints": "Return concise grounded summary and at most 3 actions.",
    }
    out = _call_with_failover(
        chain,
        [
            {
                "role": "system",
                "content": "You are a Snowflake FinOps copilot. Return strict JSON keys: summary, recommended_actions, assumptions.",
            },
            {"role": "user", "content": json.dumps(payload)},
        ],
    )
    if not out.get("ok"):
        return {"ok": False, "errors": out.get("errors", [])}
    return {"ok": True, "provider": out.get("provider"), "final": out.get("response", {})}


def _normalize_llm_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    current = payload
    # Some models nest the full response under summary as an object.
    for _ in range(2):
        summary_val = current.get("summary")
        if isinstance(summary_val, dict) and (
            "summary" in summary_val or "recommended_actions" in summary_val or "assumptions" in summary_val
        ):
            current = summary_val
            continue
        break
    return current


def _actions_relevant(intent: str, rows: List[Dict[str, Any]]) -> bool:
    intent = (intent or "").lower().strip()
    if intent == "best_jobs":
        return False
    if intent in {"heavy_jobs", "cost_spike", "spill", "pruning", "warehouse_idle", "recommendations"}:
        return True
    if intent == "pipe":
        return any(_to_float(r.get("FAILURE_COUNT") or r.get("failure_count")) > 0 for r in rows)
    if intent == "tasks":
        return any(_to_float(r.get("FAILED_COUNT") or r.get("failed_count")) > 0 for r in rows)
    return False


def _retrieve(intent: str, time_range: str):
    cache_key = f"{intent}:{time_range}"
    cached = _RETRIEVE_CACHE.get(cache_key)
    now = time()
    if cached and (now - cached.get("ts", 0)) <= _RETRIEVE_CACHE_TTL_SECONDS:
        return cached["rows"], cached["actions"], cached["sql"]

    tf = _time_filter_sql(time_range)
    conn = get_conn()
    try:
        _ensure_bootstrap_once(conn)
        with conn.cursor() as cur:
            if intent in ("heavy_jobs", "cost_spike"):
                cur.execute(
                    f"""
                    SELECT query_tag, warehouse_name, user_name, query_count, credits_total, elapsed_seconds, scanned_gb
                    FROM COST_COPILOT.V_HEAVY_TAGS_7D
                    WHERE last_seen >= {tf}
                    ORDER BY credits_total DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.V_HEAVY_TAGS_7D ORDER BY credits_total DESC LIMIT 20;"
            elif intent == "best_jobs":
                cur.execute(
                    f"""
                    WITH perf AS (
                      SELECT
                        COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
                        COALESCE(q.warehouse_name, 'UNKNOWN') AS warehouse_name,
                        COALESCE(q.user_name, 'UNKNOWN') AS user_name,
                        COUNT(*) AS query_count,
                        AVG(COALESCE(q.total_elapsed_time, 0)) / 1000 AS avg_elapsed_seconds,
                        AVG(COALESCE(q.bytes_scanned, 0)) / POW(1024, 3) AS avg_scanned_gb,
                        AVG(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0)) AS avg_credits
                      FROM COST_COPILOT.FACT_QUERY q
                      LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
                      WHERE q.start_time >= {tf}
                        AND COALESCE(q.user_name, '') <> 'SYSTEM'
                        AND COALESCE(q.query_tag, '') <> ''
                      GROUP BY 1,2,3
                    )
                    SELECT *
                    FROM perf
                    WHERE query_count >= 2
                    ORDER BY avg_elapsed_seconds ASC, avg_scanned_gb ASC, avg_credits ASC, query_count DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = (
                    "SELECT query_tag, warehouse_name, user_name, query_count, avg_elapsed_seconds, avg_scanned_gb, avg_credits "
                    "FROM (SELECT COALESCE(q.query_tag,'UNTAGGED') query_tag, COALESCE(q.warehouse_name,'UNKNOWN') warehouse_name, "
                    "COALESCE(q.user_name,'UNKNOWN') user_name, COUNT(*) query_count, AVG(COALESCE(q.total_elapsed_time,0))/1000 avg_elapsed_seconds, "
                    "AVG(COALESCE(q.bytes_scanned,0))/POW(1024,3) avg_scanned_gb, "
                    "AVG(COALESCE(c.credits_attributed_compute,0)+COALESCE(c.credits_used_query_acceleration,0)) avg_credits "
                    "FROM COST_COPILOT.FACT_QUERY q LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id=c.query_id "
                    "GROUP BY 1,2,3) ORDER BY avg_elapsed_seconds ASC LIMIT 20;"
                )
            elif intent == "spill":
                cur.execute(
                    f"""
                    SELECT query_id, query_tag, warehouse_name, spill_gb, credits_total, start_time
                    FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS
                    WHERE start_time >= {tf}
                      AND spill_gb > 0
                    ORDER BY spill_gb DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = (
                    "SELECT query_id, query_tag, warehouse_name, spill_gb, credits_total "
                    "FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS WHERE spill_gb > 0 ORDER BY spill_gb DESC LIMIT 50;"
                )
            elif intent == "pruning":
                cur.execute(
                    f"""
                    SELECT query_id, query_tag, warehouse_name, pruning_ratio, scanned_gb, credits_total, start_time
                    FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS
                    WHERE start_time >= {tf}
                      AND (pruning_ratio > 0.8 OR scanned_gb > 10)
                    ORDER BY scanned_gb DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = (
                    "SELECT query_id, query_tag, pruning_ratio, scanned_gb FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS "
                    "WHERE pruning_ratio > 0.8 OR scanned_gb > 10 ORDER BY scanned_gb DESC LIMIT 50;"
                )
            elif intent == "warehouse_idle":
                cur.execute(
                    f"""
                    SELECT warehouse_name, usage_day, credits_used, avg_running, avg_queued
                    FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS
                    WHERE usage_day >= DATE({tf})
                    ORDER BY credits_used DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = (
                    "SELECT * FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS "
                    "WHERE avg_running < 0.3 ORDER BY credits_used DESC LIMIT 20;"
                )
            elif intent == "pipe":
                cur.execute(
                    """
                    SELECT usage_date, pipe_name, credits_used, failure_count, success_count, small_file_loads
                    FROM COST_COPILOT.V_PIPE_HEALTH_SIGNALS
                    ORDER BY failure_count DESC, credits_used DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.V_PIPE_HEALTH_SIGNALS ORDER BY failure_count DESC LIMIT 30;"
            elif intent == "tasks":
                cur.execute(
                    f"""
                    SELECT usage_date, task_name, database_name, schema_name, state, run_count, failed_count, avg_duration_seconds
                    FROM COST_COPILOT.FACT_TASK_DAILY
                    WHERE usage_date >= DATE({tf})
                    ORDER BY failed_count DESC, run_count DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.FACT_TASK_DAILY ORDER BY usage_date DESC, failed_count DESC LIMIT 50;"
            else:
                cur.execute(
                    """
                    SELECT rec_id, rule_name, recommendation_type, object_name, risk, est_savings, ai_summary, confidence_score
                    FROM COST_COPILOT.V_TOP_RECOMMENDATIONS
                    ORDER BY created_at DESC
                    LIMIT 10
                    """
                )
                rows = _rows_to_dicts(cur)
                sql = "SELECT * FROM COST_COPILOT.V_TOP_RECOMMENDATIONS ORDER BY created_at DESC LIMIT 20;"

            cur.execute(
                """
                SELECT rec_id, recommendation_type, object_name, risk, suggested_fix, ddl_sql
                FROM COST_COPILOT.RECOMMENDATIONS
                ORDER BY created_at DESC
                LIMIT 5
                """
            )
            actions = _rows_to_dicts(cur)
            _RETRIEVE_CACHE[cache_key] = {"ts": now, "rows": rows, "actions": actions, "sql": sql}
            return rows, actions, sql
    finally:
        conn.close()


def _build_answer(intent: str, question: str, rows: List[Dict], actions: List[Dict], sql: str):
    if not rows:
        summary = "No matching copilot evidence found for the selected time range."
    else:
        top = rows[0]
        if intent == "best_jobs":
            summary = (
                f"Most efficient job right now is '{top.get('QUERY_TAG') or top.get('query_tag')}'. "
                f"It averages ~{round(_to_float(top.get('AVG_ELAPSED_SECONDS') or top.get('avg_elapsed_seconds')), 2)}s runtime "
                f"with ~{round(_to_float(top.get('AVG_CREDITS') or top.get('avg_credits')), 4)} credits/query."
            )
        elif intent in ("heavy_jobs", "cost_spike"):
            top_object = (
                top.get("QUERY_TAG")
                or top.get("query_tag")
                or top.get("WAREHOUSE_NAME")
                or top.get("warehouse_name")
                or top.get("USER_NAME")
                or top.get("user_name")
                or "UNIDENTIFIED_OBJECT"
            )
            elapsed = round(_to_float(top.get("ELAPSED_SECONDS") or top.get("elapsed_seconds")), 2)
            scanned = round(_to_float(top.get("SCANNED_GB") or top.get("scanned_gb")), 2)
            credits = round(_to_float(top.get("CREDITS_TOTAL") or top.get("credits_total")), 4)
            summary = (
                f"Top expensive/slow workload is '{top_object}' "
                f"(credits ~{credits}, elapsed ~{elapsed}s, scanned ~{scanned} GB) in the selected window."
            )
        elif intent == "spill":
            summary = (
                f"Highest spill query is '{top.get('QUERY_ID') or top.get('query_id')}' "
                f"with ~{round(_to_float(top.get('SPILL_GB') or top.get('spill_gb')), 2)} GB spilled."
            )
        elif intent == "warehouse_idle":
            summary = (
                f"Most idle warehouse appears to be '{top.get('WAREHOUSE_NAME') or top.get('warehouse_name')}' "
                f"with low utilization and measurable idle burn."
            )
        else:
            summary = f"Found {len(rows)} evidence rows for '{question}'."

    offenders = []
    evidence_refs = []
    for row in rows[:5]:
        offenders.append(row)
        for field in ("QUERY_ID", "QUERY_TAG", "WAREHOUSE_NAME", "OBJECT_NAME", "PIPE_NAME", "TASK_NAME"):
            if field in row and row[field]:
                evidence_refs.append(str(row[field]))
    evidence_refs = sorted(set(evidence_refs))[:20]

    include_actions = _actions_relevant(intent, rows)
    return {
        "summary": summary,
        "top_offenders": offenders,
        "recommended_actions": actions[:5] if include_actions else [],
        "evidence_references": evidence_refs,
        "sql_snippets": [
            sql,
            "SELECT * FROM COST_COPILOT.V_TOP_RECOMMENDATIONS ORDER BY created_at DESC LIMIT 20;",
        ],
    }


def _build_smart_answer(
    intent: str,
    question: str,
    rows: List[Dict],
    actions: List[Dict],
    sql: str,
    history: Optional[List[Dict[str, Any]]] = None,
    llm_mode: Optional[str] = None,
    llm_provider: Optional[str] = None,
):
    base = _build_answer(intent, question, rows, actions, sql)
    if rows:
        key_metrics = {
            "rows_returned": len(rows),
            "top_object": rows[0].get("QUERY_TAG")
            or rows[0].get("WAREHOUSE_NAME")
            or rows[0].get("PIPE_NAME")
            or rows[0].get("OBJECT_NAME"),
        }
    else:
        key_metrics = {"rows_returned": 0, "top_object": None}

    llm_payload = {
        "intent": intent,
        "question": question,
        "rows": _json_safe(rows[:8]),
        "actions": _json_safe(actions[:5]),
        "history": history or [],
    }
    provider_requested = _provider_from_inputs(llm_mode, llm_provider)
    resolved = _resolve_chat_provider(provider_requested)
    provider = resolved.get("provider")
    chain = _provider_chain(provider)
    chain_agentic = chain[:1] if chain else []
    llm = None
    llm_diagnostics = {
        "provider": provider_requested or "none",
        "chat_provider": provider or "none",
        "reason": "",
        "configured": False,
        "enabled": bool(provider),
    }
    if resolved.get("reason") and resolved.get("reason") != "OK":
        llm_diagnostics["reason"] = str(resolved.get("reason"))
    if provider:
        cfg = _provider_config(provider)
        llm_diagnostics["configured"] = bool(cfg.get("api_key"))
        if not cfg.get("api_key"):
            llm_diagnostics["reason"] = f"{provider.upper()}_API_KEY_MISSING"

    rag_chunks = _rag_fetch(question, limit=8)
    agentic = _run_agentic_methodology(
        question=question,
        intent=intent,
        rows=rows,
        actions=actions,
        history=history,
        chain=chain_agentic,
        rag_chunks=rag_chunks,
    )
    llm = agentic.get("final") if agentic.get("ok") else None
    degraded_direct = None
    if not llm and chain:
        degraded_direct = _run_direct_llm_grounded(
            question=question,
            intent=intent,
            rows=rows,
            actions=actions,
            history=history,
            rag_chunks=rag_chunks,
            chain=chain,
        )
        if degraded_direct.get("ok"):
            llm = degraded_direct.get("final")
    rescue_minimal = None
    if not llm and chain:
        rescue_minimal = _run_minimal_llm_rescue(question=question, intent=intent, rows=rows, chain=chain)
        if rescue_minimal.get("ok"):
            llm = rescue_minimal.get("final")
    llm = _normalize_llm_payload(llm)
    if llm and isinstance(llm, dict):
        llm_summary = llm.get("summary") or agentic.get("analysis", {}).get("plain_english") or base["summary"]
        if isinstance(llm_summary, str):
            candidate = llm_summary.strip()
            if candidate.startswith("{") and '"summary"' in candidate:
                try:
                    parsed_candidate = _parse_llm_json(candidate)
                    if isinstance(parsed_candidate, dict) and parsed_candidate.get("summary"):
                        llm_summary = parsed_candidate.get("summary")
                        if _actions_relevant(intent, rows) and isinstance(parsed_candidate.get("recommended_actions"), list):
                            base["recommended_actions"] = parsed_candidate["recommended_actions"]
                except Exception:
                    pass
        if not isinstance(llm_summary, str):
            llm_summary = json.dumps(_json_safe(llm_summary))
        llm_summary = llm_summary.strip()
        if len(llm_summary) > 700:
            llm_summary = llm_summary[:700].rstrip() + "..."
        base["summary"] = llm_summary or base["summary"]
        llm_actions = llm.get("recommended_actions")
        if _actions_relevant(intent, rows) and isinstance(llm_actions, list) and llm_actions:
            base["recommended_actions"] = llm_actions
        base["assumptions"] = llm.get("assumptions", [])
        if rag_chunks:
            base["assumptions"] = list(base.get("assumptions") or []) + ["RAG context from COST_COPILOT.RAG_CHUNKS was used."]
        if rescue_minimal and rescue_minimal.get("ok"):
            base["answer_mode"] = f"{rescue_minimal.get('provider', provider)}_rescue_grounded"
            base["assumptions"] = list(base.get("assumptions") or []) + [
                "Multi-agent and direct paths degraded; minimal grounded rescue path was used."
            ]
        elif degraded_direct and degraded_direct.get("ok"):
            base["answer_mode"] = f"{degraded_direct.get('provider', provider)}_direct_grounded"
            base["assumptions"] = list(base.get("assumptions") or []) + [
                "Multi-agent pipeline degraded; direct grounded LLM path was used."
            ]
        else:
            base["answer_mode"] = f"{agentic.get('provider', provider)}_multi_agent_grounded"
        if llm_diagnostics.get("reason") and llm_diagnostics["reason"] != "OK":
            llm_diagnostics["reason"] = f"{llm_diagnostics['reason']}_OK"
        else:
            llm_diagnostics["reason"] = "OK"
        if not (degraded_direct and degraded_direct.get("ok")) and not (rescue_minimal and rescue_minimal.get("ok")):
            base["agent_methodology"] = {
                "pipeline": ["planner", "analyst", "responder"],
                "used_rag_chunks": len(rag_chunks),
                "planner": agentic.get("planner", {}),
                "analysis": agentic.get("analysis", {}),
            }
    else:
        if not provider:
            if not llm_diagnostics["reason"]:
                llm_diagnostics["reason"] = "LLM_MODE_OFF"
        elif not llm_diagnostics["reason"]:
            llm_diagnostics["reason"] = (agentic.get("reason") or "PROVIDER_UNREACHABLE_OR_TIMEOUT")
        base["assumptions"] = [f"LLM fallback to deterministic mode: {llm_diagnostics['reason']}."]
        base["answer_mode"] = "deterministic"
    base["key_metrics"] = key_metrics
    base["llm_diagnostics"] = llm_diagnostics
    base["follow_up_suggestions"] = [
        "Show me same jobs for 24h.",
        "Which of these jobs have spill?",
        "Give me top recommended fixes for these jobs.",
    ]
    return base


def _build_dashboard_report(range_value: str, summary: Dict[str, Any], trend: List[Dict[str, Any]], top_jobs: List[Dict[str, Any]]) -> str:
    total_credits = _to_float(summary.get("total_credits"))
    scanned_tb = _to_float(summary.get("scanned_tb"))
    query_count = int(_to_float(summary.get("query_count")))
    elapsed_hours = _to_float(summary.get("elapsed_hours"))

    direction = "stable"
    if len(trend) >= 2:
        first = _to_float(trend[0].get("credits"))
        last = _to_float(trend[-1].get("credits"))
        if last > first * 1.15:
            direction = "increasing"
        elif last < first * 0.85:
            direction = "decreasing"

    top_text = "No clear top cost driver yet."
    if top_jobs:
        top = top_jobs[0]
        top_text = (
            f"Top cost driver is '{top.get('query_tag', 'UNTAGGED')}' on warehouse "
            f"'{top.get('warehouse_name', 'UNKNOWN')}' at about {_to_float(top.get('credits_total')):.4f} credits."
        )

    return (
        f"In the selected window ({range_value}), Snowflake consumed about {total_credits:.2f} credits across "
        f"{query_count} queries, scanning {scanned_tb:.2f} TB in {elapsed_hours:.1f} elapsed query-hours. "
        f"Credit trend is {direction}. {top_text}"
    )


@app.get("/api/dashboard")
def dashboard(range: str = Query(default="7d"), bucket: str = Query(default="daily")):
    cache_key = f"{range}:{bucket}"
    cached = _DASHBOARD_CACHE.get(cache_key)
    now = time()
    if cached and (now - cached.get("ts", 0)) <= _DASHBOARD_CACHE_TTL_SECONDS:
        return cached["payload"]

    tf = _time_filter_sql(range)
    bucket_expr = "DATE_TRUNC('DAY', q.start_time)" if bucket.lower() == "daily" else "DATE_TRUNC('WEEK', q.start_time)"
    summary = _run_query_dict(
        f"""
        SELECT
          COALESCE(SUM(COALESCE(c.credits_attributed_compute,0) + COALESCE(c.credits_used_query_acceleration,0)),0) AS total_credits,
          COALESCE(SUM(COALESCE(q.total_elapsed_time,0))/1000/3600,0) AS elapsed_hours,
          COALESCE(SUM(COALESCE(q.bytes_scanned,0))/POW(1024,4),0) AS scanned_tb,
          COUNT(*) AS query_count
        FROM COST_COPILOT.FACT_QUERY q
        LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
        WHERE q.start_time >= {tf}
        """,
        bootstrap=False,
    )
    trend = _run_query_dict(
        f"""
        SELECT
          {bucket_expr} AS ts_bucket,
          COALESCE(SUM(COALESCE(c.credits_attributed_compute,0) + COALESCE(c.credits_used_query_acceleration,0)),0) AS credits
        FROM COST_COPILOT.FACT_QUERY q
        LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
        WHERE q.start_time >= {tf}
        GROUP BY 1
        ORDER BY 1
        """,
        bootstrap=False,
    )
    top_jobs = _run_query_dict(
        f"""
        SELECT
          COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
          COALESCE(q.warehouse_name, 'UNKNOWN') AS warehouse_name,
          COALESCE(q.user_name, 'UNKNOWN') AS user_name,
          ROUND(SUM(COALESCE(c.credits_attributed_compute,0) + COALESCE(c.credits_used_query_acceleration,0)),4) AS credits_total,
          ROUND(SUM(COALESCE(q.total_elapsed_time,0))/1000,2) AS elapsed_seconds,
          ROUND(SUM(COALESCE(q.bytes_scanned,0))/POW(1024,3),2) AS scanned_gb
        FROM COST_COPILOT.FACT_QUERY q
        LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
        WHERE q.start_time >= {tf}
        GROUP BY 1,2,3
        ORDER BY credits_total DESC, elapsed_seconds DESC
        LIMIT 10
        """,
        bootstrap=False,
    )
    summary_obj = summary[0] if summary else {}
    report = _build_dashboard_report(range, summary_obj, trend, top_jobs)
    payload = {"range": range, "bucket": bucket, "summary": summary_obj, "trend": trend, "top_jobs": top_jobs, "plain_english_report": report}
    _DASHBOARD_CACHE[cache_key] = {"ts": now, "payload": payload}
    return payload


@app.get("/health")
def health():
    return {"status": "ok"}


def _provider_health() -> Dict[str, Any]:
    active = os.environ.get("LLM_PROVIDER", "openrouter").lower()
    configured = {
        "openrouter": bool(os.environ.get("OPENROUTER_API_KEY")),
        "groq": bool(os.environ.get("GROQ_API_KEY")),
        "cursor": bool(os.environ.get("CURSOR_API_KEY")),
    }
    return {
        "active_provider": active,
        "openrouter_configured": configured["openrouter"],
        "groq_configured": configured["groq"],
        "cursor_configured": configured["cursor"],
        "active_provider_configured": configured.get(active, False),
    }


def _collect_repo_index(max_files: int = 24, max_chars: int = 14000) -> str:
    repo_root = Path(__file__).resolve().parents[1]
    allowed_suffixes = {".py", ".sql", ".md"}
    snippets: List[str] = []
    used = 0
    files = sorted([p for p in repo_root.rglob("*") if p.is_file() and p.suffix.lower() in allowed_suffixes])
    for path in files[:max_files]:
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")[:900]
        except Exception:
            continue
        rel = path.relative_to(repo_root).as_posix()
        block = f"\n# File: {rel}\n{content}\n"
        if used + len(block) > max_chars:
            break
        snippets.append(block)
        used += len(block)
    return "\n".join(snippets)


def _generate_fix_sql_with_llm(
    rec: Dict[str, Any],
    provider: str,
    *,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: Optional[str] = None,
) -> str:
    prompt = {
        "objective": "Generate safe Snowflake SQL fix from recommendation.",
        "rec_id": rec.get("rec_id"),
        "object_name": rec.get("object_name"),
        "object_type": rec.get("object_type"),
        "rule_name": rec.get("rule_name"),
        "risk": rec.get("risk"),
        "suggested_fix": rec.get("suggested_fix"),
        "evidence_json": rec.get("evidence_json"),
        "existing_ddl_sql": rec.get("ddl_sql"),
        "repo_index_excerpt": _collect_repo_index(),
    }
    llm = _call_chat_provider(
        provider,
        [
            {
                "role": "system",
                "content": (
                    "You are a Snowflake remediation engineer. Return strict JSON with keys: "
                    "summary, recommended_actions, assumptions, ddl_sql, rollback_sql."
                ),
            },
            {"role": "user", "content": json.dumps(prompt, default=str)},
        ],
        api_key_override=api_key,
        model_override=model,
        base_url_override=base_url,
    )
    if not llm:
        return ""
    ddl = llm.get("ddl_sql")
    return ddl.strip() if isinstance(ddl, str) else ""


def _is_concrete_sql_fix(sql_text: str) -> bool:
    if not sql_text:
        return False
    cleaned_lines = []
    for raw in sql_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("--"):
            continue
        cleaned_lines.append(line)
    if not cleaned_lines:
        return False
    normalized = " ".join(cleaned_lines).lower()
    if "could not generate ddl" in normalized or "no executable sql" in normalized:
        return False
    sql_starts = (
        "alter ",
        "create ",
        "replace ",
        "drop ",
        "update ",
        "merge ",
        "insert ",
        "delete ",
        "truncate ",
        "call ",
        "grant ",
        "revoke ",
    )
    return normalized.startswith(sql_starts)


def _run_cmd(cmd: List[str], cwd: Path, timeout_s: int = 45) -> Dict[str, Any]:
    env = os.environ.copy()
    if env.get("GITHUB_TOKEN") and not env.get("GH_TOKEN"):
        env["GH_TOKEN"] = env["GITHUB_TOKEN"]
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        p = subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, env=env, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return {"ok": False, "stdout": "", "stderr": f"command_timeout_{timeout_s}s", "code": 124}
    token = env.get("GITHUB_TOKEN") or env.get("GH_TOKEN") or ""
    stdout = p.stdout.strip()
    stderr = p.stderr.strip()
    if token:
        stdout = stdout.replace(token, "***")
        stderr = stderr.replace(token, "***")
    return {"ok": p.returncode == 0, "stdout": stdout, "stderr": stderr, "code": p.returncode}


def _github_repo_slug(repo_url: Optional[str]) -> Optional[str]:
    if not repo_url:
        return None
    cleaned = repo_url.strip()
    cleaned = cleaned.split("#", 1)[0].split("?", 1)[0]
    m = re.search(r"github\.com[:/]+([^/]+)/([^/]+?)(?:\.git)?/?$", cleaned)
    if not m:
        return None
    return f"{m.group(1)}/{m.group(2)}"


def _github_api_create_pr(repo_url: Optional[str], branch_name: str, base_branch: str, title: str, body: str) -> Dict[str, Any]:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    slug = _github_repo_slug(repo_url)
    if not token or not slug:
        return {"ok": False, "error": "missing_token_or_repo_slug"}
    owner = slug.split("/", 1)[0]
    payload = {"title": title, "head": f"{owner}:{branch_name}", "base": base_branch, "body": body}
    req = request.Request(
        f"https://api.github.com/repos/{slug}/pulls",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=25) as resp:
            out = json.loads(resp.read().decode("utf-8"))
            return {"ok": True, "pr_url": out.get("html_url"), "raw": out}
    except error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            body = ""
        return {"ok": False, "error": f"{exc} {body[:1200]}".strip()}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _github_api_repo_check(repo_url: Optional[str]) -> Dict[str, Any]:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    slug = _github_repo_slug(repo_url)
    if not token or not slug:
        return {"ok": False, "error": "missing_token_or_repo_slug", "slug": slug}
    req = request.Request(
        f"https://api.github.com/repos/{slug}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return {"ok": True, "slug": slug, "default_branch": body.get("default_branch")}
    except Exception as exc:
        return {"ok": False, "slug": slug, "error": str(exc)}


def _create_artifact_pr(rec_id: str, branch_name: str, title: str, body: str, files_to_add: List[str]) -> Dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[1]
    repo_url = os.environ.get("GITHUB_REPO_URL")
    configured_base = os.environ.get("GITHUB_BASE_BRANCH", "main")
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    push_url = repo_url
    if repo_url and token and repo_url.startswith("https://github.com/"):
        push_url = repo_url.replace("https://", f"https://x-access-token:{token}@")
    out = {"branch": branch_name, "pr_url": None, "steps": []}
    repo_check = _github_api_repo_check(repo_url)
    out["steps"].append({"tool": "repo_check", **repo_check})
    effective_base = configured_base
    if repo_check.get("ok") and repo_check.get("default_branch"):
        effective_base = str(repo_check.get("default_branch"))
    out["steps"].append(_run_cmd(["git", "checkout", "-B", branch_name], repo_root))
    out["steps"].append(_run_cmd(["git", "add", "-f", *files_to_add], repo_root))
    commit_res = _run_cmd(["git", "commit", "-m", title, "--", *files_to_add], repo_root)
    if (not commit_res.get("ok")) and ("nothing to commit" in (commit_res.get("stdout", "") + commit_res.get("stderr", "")).lower()):
        commit_res = {"ok": True, "stdout": "nothing_to_commit", "stderr": "", "code": 0}
    out["steps"].append(commit_res)
    if not commit_res.get("ok"):
        return out
    if push_url:
        push_res = _run_cmd(["git", "push", "-u", push_url, f"HEAD:{branch_name}"], repo_root)
        if not push_res["ok"]:
            push_res = _run_cmd(["git", "push", "-u", "origin", branch_name], repo_root)
    else:
        push_res = _run_cmd(["git", "push", "-u", "origin", branch_name], repo_root)
    out["steps"].append(push_res)
    if not push_res["ok"]:
        return out

    slug = _github_repo_slug(repo_url)
    owner = (slug.split("/", 1)[0] if slug else "")
    head_ref = f"{owner}:{branch_name}" if owner else branch_name
    pr_res = _run_cmd(
        [
            "gh",
            "pr",
            "create",
            "--repo",
            slug or "",
            "--base",
            effective_base,
            "--head",
            head_ref,
            "--title",
            title,
            "--body",
            body,
        ],
        repo_root,
    )
    out["steps"].append(pr_res)
    if pr_res["ok"]:
        lines = [x.strip() for x in (pr_res["stdout"] or "").splitlines() if x.strip()]
        if lines:
            out["pr_url"] = lines[-1]
            return out

    api_pr = _github_api_create_pr(repo_url, branch_name, effective_base, title, body)
    out["steps"].append({"ok": api_pr.get("ok"), "stdout": api_pr.get("pr_url") or "", "stderr": api_pr.get("error") or "", "code": 0 if api_pr.get("ok") else 1, "tool": "github_api"})
    if api_pr.get("ok"):
        out["pr_url"] = api_pr.get("pr_url")
    return out


def _launch_cursor_agent(prompt_text: str, branch_name: str, base_branch: str) -> Dict[str, Any]:
    api_key = os.environ.get("CURSOR_API_KEY")
    repository = os.environ.get("CURSOR_REPOSITORY") or os.environ.get("GITHUB_REPO_URL")
    if not api_key or not repository:
        return {"ok": False, "error": "CURSOR_API_KEY or CURSOR_REPOSITORY/GITHUB_REPO_URL missing"}

    payload = {
        "source": {"repository": repository, "ref": base_branch},
        "target": {"branchName": branch_name, "autoCreatePr": True, "openAsCursorGithubApp": True},
        "prompt": {"text": prompt_text},
    }
    auth = b64encode(f"{api_key}:".encode("utf-8")).decode("utf-8")
    req = request.Request(
        f"{os.environ.get('CURSOR_BASE_URL', 'https://api.cursor.com').rstrip('/')}/v0/agents",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return {"ok": True, "agent": body}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _rag_rows_to_chunks(source_type: str, source_name: str, rows: List[Dict[str, Any]], max_chunks: int = 400) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows[:max_chunks]):
        txt = json.dumps(_json_safe(row), default=str)
        chunks.append(
            {
                "chunk_id": str(uuid.uuid4()),
                "doc_id": f"{source_type}:{source_name}",
                "source_type": source_type,
                "source_name": source_name,
                "chunk_text": txt[:3900],
                "metadata_json": json.dumps({"source_type": source_type, "source_name": source_name, "row_index": idx}),
            }
        )
    return chunks


@app.post("/api/rag/reindex")
def rag_reindex(req: RagReindexRequest):
    tf = _time_filter_sql(req.time_range or "30d")
    conn = get_conn()
    inserted = 0
    try:
        _ensure_bootstrap_once(conn)
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute("DELETE FROM COST_COPILOT.RAG_CHUNKS")
            cur.execute("DELETE FROM COST_COPILOT.RAG_DOCUMENTS")

            sources = [
                (
                    "view",
                    "V_TOP_RECOMMENDATIONS",
                    f"SELECT * FROM COST_COPILOT.V_TOP_RECOMMENDATIONS ORDER BY created_at DESC LIMIT {int(req.max_rows_per_source)}",
                ),
                (
                    "view",
                    "V_HEAVY_TAGS_7D",
                    f"SELECT * FROM COST_COPILOT.V_HEAVY_TAGS_7D WHERE last_seen >= {tf} LIMIT {int(req.max_rows_per_source)}",
                ),
                (
                    "view",
                    "V_QUERY_WASTE_SIGNALS",
                    f"SELECT * FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS WHERE start_time >= {tf} LIMIT {int(req.max_rows_per_source)}",
                ),
                (
                    "view",
                    "V_WAREHOUSE_IDLE_SIGNALS",
                    f"SELECT * FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS WHERE usage_day >= DATE({tf}) LIMIT {int(req.max_rows_per_source)}",
                ),
                (
                    "table",
                    "ANOMALIES",
                    f"SELECT * FROM COST_COPILOT.ANOMALIES ORDER BY detected_at DESC LIMIT {int(req.max_rows_per_source)}",
                ),
            ]
            for source_type, source_name, sql in sources:
                cur.execute(sql)
                rows = [{k.lower(): _json_safe(v) for k, v in r.items()} for r in cur.fetchall()]
                cur.execute(
                    """
                    INSERT INTO COST_COPILOT.RAG_DOCUMENTS (doc_id, source_type, source_name, doc_text, metadata_json, created_at)
                    SELECT %(doc_id)s, %(source_type)s, %(source_name)s, %(doc_text)s, PARSE_JSON(%(metadata_json)s), CURRENT_TIMESTAMP()
                    """,
                    {
                        "doc_id": f"{source_type}:{source_name}",
                        "source_type": source_type,
                        "source_name": source_name,
                        "doc_text": json.dumps(rows[:20])[:14000],
                        "metadata_json": json.dumps({"row_count": len(rows)}),
                    },
                )
                chunks = _rag_rows_to_chunks(source_type, source_name, rows, max_chunks=req.max_rows_per_source)
                for ch in chunks:
                    cur.execute(
                        """
                        INSERT INTO COST_COPILOT.RAG_CHUNKS
                        (chunk_id, doc_id, source_type, source_name, chunk_text, metadata_json, created_at)
                        SELECT %(chunk_id)s, %(doc_id)s, %(source_type)s, %(source_name)s, %(chunk_text)s, PARSE_JSON(%(metadata_json)s), CURRENT_TIMESTAMP()
                        """,
                        ch,
                    )
                inserted += len(chunks)
            conn.commit()
    finally:
        conn.close()
    return {"ok": True, "inserted_chunks": inserted, "time_range": req.time_range}


@app.get("/api/rag/status")
def rag_status():
    return {
        "documents": _run_query_dict("SELECT COUNT(*) AS total_docs FROM COST_COPILOT.RAG_DOCUMENTS", bootstrap=False),
        "chunks": _run_query_dict("SELECT COUNT(*) AS total_chunks FROM COST_COPILOT.RAG_CHUNKS", bootstrap=False),
        "recent_sources": _run_query_dict(
            "SELECT source_type, source_name, COUNT(*) AS chunks FROM COST_COPILOT.RAG_CHUNKS GROUP BY 1,2 ORDER BY chunks DESC LIMIT 10",
            bootstrap=False,
        ),
    }


@app.get("/api/llm/providers")
def llm_providers():
    return _provider_health()


@app.post("/api/llm/test")
def llm_test(req: LLMTestRequest):
    provider = (req.provider or "openrouter").lower().strip()
    if provider == "cursor":
        return {"ok": bool(req.api_key or os.environ.get("CURSOR_API_KEY")), "provider": "cursor", "note": "Cursor chat test is config-only. Use remediation action to launch agent."}

    t0 = time()
    resp = _call_chat_provider(
        provider,
        [
            {"role": "system", "content": "Return JSON only."},
            {"role": "user", "content": '{"summary":"connection_ok","recommended_actions":[],"assumptions":[]}'},
        ],
        api_key_override=req.api_key,
        model_override=req.model,
        base_url_override=req.base_url,
    )
    elapsed_ms = int((time() - t0) * 1000)
    return {"ok": bool(resp), "provider": provider, "latency_ms": elapsed_ms, "response": resp or {}}


@app.post("/chat")
def chat(req: ChatRequest):
    try:
        provider_requested = _provider_from_inputs(req.llm_mode, req.llm_provider)
        resolved = _resolve_chat_provider(provider_requested)
        chain = _provider_chain(resolved.get("provider"))
        intent = _classify_intent(req.question, req.history)
        llm_intent = _classify_intent_agentic(req.question, req.history, chain)
        intent_mode = "rule_fallback"
        if llm_intent:
            intent = llm_intent
            intent_mode = "llm_agentic"
        intent = _guardrail_intent(req.question, intent) or intent
        rows, actions, sql = _retrieve(intent, req.time_range or "7d")
        payload = _build_smart_answer(
            intent,
            req.question,
            rows,
            actions,
            sql,
            history=req.history,
            llm_mode=req.llm_mode,
            llm_provider=req.llm_provider,
        )
        payload["intent"] = intent
        payload["intent_mode"] = intent_mode
        payload["time_range"] = req.time_range
        payload["grounding_note"] = "Response is generated from COST_COPILOT facts/views only."
        return payload
    except KeyError as exc:
        return {
            "summary": f"Configuration error: missing environment variable {exc}. Load .env or set variable.",
            "top_offenders": [],
            "recommended_actions": [],
            "evidence_references": [],
            "sql_snippets": [],
            "answer_mode": "error",
        }
    except Exception as exc:
        return {
            "summary": f"Chat processing failed: {exc}",
            "top_offenders": [],
            "recommended_actions": [],
            "evidence_references": [],
            "sql_snippets": [],
            "answer_mode": "error",
        }


@app.post("/api/chat")
def chat_v2(req: ChatRequest):
    return chat(req)


@app.get("/api/anomalies")
def anomalies(limit: int = Query(default=25, le=200), severity: Optional[str] = None):
    return {
        "items": _run_query_dict(
            """
            SELECT anomaly_id, metric_name, object_type, object_name, severity, anomaly_score,
                   observed_value, baseline_value, suspected_cause, detected_at, status
            FROM COST_COPILOT.ANOMALIES
            WHERE (%(severity)s IS NULL OR severity = %(severity)s)
            ORDER BY detected_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit, "severity": severity},
        )
    }


@app.get("/api/recommendations")
def recommendations(limit: int = Query(default=25, le=200), risk: Optional[str] = None):
    return {
        "items": _run_query_dict(
            """
            SELECT r.rec_id, r.rule_name, r.recommendation_type, r.object_type, r.object_name,
                   r.risk, r.est_savings, r.status, r.created_at, s.final_quality_score
            FROM COST_COPILOT.RECOMMENDATIONS r
            LEFT JOIN COST_COPILOT.RECOMMENDATION_SCORES s ON r.rec_id = s.rec_id
            WHERE (%(risk)s IS NULL OR r.risk = %(risk)s)
            ORDER BY r.created_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit, "risk": risk},
        )
    }


@app.post("/api/recommendations/{rec_id}/apply_and_pr")
def apply_and_pr(rec_id: str, req: ApplyRecommendationRequest):
    conn = get_conn()
    try:
        _ensure_bootstrap_once(conn)
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(
                """
                SELECT rec_id, rule_name, object_type, object_name, recommendation_type, risk,
                       evidence_json, suggested_fix, ddl_sql, rollback_sql, status, est_savings
                FROM COST_COPILOT.RECOMMENDATIONS
                WHERE rec_id = %(rec_id)s
                """,
                {"rec_id": rec_id},
            )
            row = cur.fetchone()
            if not row:
                return {"ok": False, "error": f"Recommendation {rec_id} not found"}
            rec = {k.lower(): _json_safe(v) for k, v in row.items()}

            generated_ddl = rec.get("ddl_sql") or _generate_fix_sql_with_llm(
                rec,
                req.provider,
                api_key=req.api_key,
                model=req.model,
                base_url=req.base_url,
            )
            generated_ddl = (generated_ddl or "").strip()
            has_concrete_fix = _is_concrete_sql_fix(generated_ddl)
            if not has_concrete_fix:
                return {
                    "ok": False,
                    "error": "NO_IMPLEMENTABLE_FIX",
                    "message": (
                        "Recommendation does not have a concrete executable SQL fix yet. "
                        "Skipping PR creation to avoid placeholder/no-op pull requests."
                    ),
                    "rec_id": rec_id,
                    "recommendation": {
                        "rule_name": rec.get("rule_name"),
                        "recommendation_type": rec.get("recommendation_type"),
                        "object_type": rec.get("object_type"),
                        "object_name": rec.get("object_name"),
                        "risk": rec.get("risk"),
                        "suggested_fix": rec.get("suggested_fix"),
                    },
                }
            if req.mode.upper() == "APPLY" and generated_ddl:
                cur.execute(generated_ddl)
                cur.execute(
                    "UPDATE COST_COPILOT.RECOMMENDATIONS SET status='APPLIED' WHERE rec_id = %(rec_id)s",
                    {"rec_id": rec_id},
                )
                conn.commit()
            elif req.mode.upper() == "APPLY" and not generated_ddl:
                return {"ok": False, "error": "No executable SQL found/generated for APPLY mode"}

    finally:
        conn.close()

    repo_root = Path(__file__).resolve().parents[1]
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    branch_name = f"copilot/remediate-{rec_id[:8]}-{ts}"

    sql_rel = f"sql/remediations/{rec_id}-{ts}.sql"
    md_rel = f"reports/remediations/{rec_id}-{ts}.md"
    (repo_root / "sql" / "remediations").mkdir(parents=True, exist_ok=True)
    (repo_root / "reports" / "remediations").mkdir(parents=True, exist_ok=True)

    sql_body = generated_ddl.strip() + "\n"
    (repo_root / sql_rel).write_text(sql_body, encoding="utf-8")
    md_body = (
        f"# Remediation {rec_id}\n\n"
        f"- Rule: {rec.get('rule_name')}\n"
        f"- Object: {rec.get('object_type')} `{rec.get('object_name')}`\n"
        f"- Risk: {rec.get('risk')}\n"
        f"- Recommendation: {rec.get('suggested_fix')}\n"
        f"- Mode: {req.mode.upper()}\n\n"
        f"## SQL\n\n```sql\n{sql_body}```\n"
    )
    (repo_root / md_rel).write_text(md_body, encoding="utf-8")

    if req.provider.lower() == "cursor":
        cursor_prompt = (
            "Apply the remediation documented in {md_rel} and {sql_rel}. "
            "Update related Snowflake SQL/Python files safely, run checks, commit changes, and open a PR."
        ).format(md_rel=md_rel, sql_rel=sql_rel)
        launch = _launch_cursor_agent(cursor_prompt, branch_name=branch_name, base_branch=req.base_branch)
        return {
            "ok": launch.get("ok", False),
            "mode": req.mode.upper(),
            "provider": "cursor",
            "rec_id": rec_id,
            "branch": branch_name,
            "cursor": launch,
            "artifacts": {"sql_file": sql_rel, "report_file": md_rel},
        }

    if not req.auto_create_pr:
        return {
            "ok": True,
            "mode": req.mode.upper(),
            "provider": req.provider,
            "rec_id": rec_id,
            "branch": None,
            "pr_url": None,
            "artifacts": {"sql_file": sql_rel, "report_file": md_rel},
        }

    pr_title = f"copilot: remediate {rec_id}"
    pr_body = (
        "## Summary\n"
        f"- Applies remediation artifacts for recommendation `{rec_id}`.\n"
        f"- Mode used: `{req.mode.upper()}`.\n"
        f"- Generated SQL is in `{sql_rel}`.\n\n"
        "## Validation\n"
        "- [ ] Review SQL safety\n"
        "- [ ] Run pipeline checks\n"
        "- [ ] Validate Snowflake metrics after apply\n"
    )
    pr = _create_artifact_pr(rec_id, branch_name, pr_title, pr_body, [sql_rel, md_rel])
    return {
        "ok": bool(pr.get("pr_url")),
        "mode": req.mode.upper(),
        "provider": req.provider,
        "rec_id": rec_id,
        "branch": pr.get("branch"),
        "pr_url": pr.get("pr_url"),
        "steps": pr.get("steps"),
        "artifacts": {"sql_file": sql_rel, "report_file": md_rel},
    }


@app.post("/api/simulate")
def simulate(req: SimulationRequest):
    from simulator import run_simulation

    sim_id = run_simulation(
        req.target_type,
        req.target_name,
        {
            "scan_reduction_pct": req.scan_reduction_pct,
            "spill_reduction_pct": req.spill_reduction_pct,
            "warehouse_resize": req.warehouse_resize,
        },
    )
    rows = _run_query_dict(
        """
        SELECT simulation_id, scenario_name, expected_credit_delta, expected_runtime_delta_pct, expected_usd_delta, confidence_score
        FROM COST_COPILOT.SIMULATION_RESULTS
        WHERE simulation_id = %(simulation_id)s
        """,
        {"simulation_id": sim_id},
    )
    return {"simulation_id": sim_id, "results": rows}


@app.get("/api/approvals")
def approvals(limit: int = Query(default=25, le=200)):
    return {
        "items": _run_query_dict(
            """
            SELECT rec_id, approved, approved_by, approved_at, approval_note, updated_at
            FROM COST_COPILOT.APPROVALS
            ORDER BY updated_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
    }


@app.post("/api/approvals")
def upsert_approval(req: ApprovalRequest):
    conn = get_conn()
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                MERGE INTO COST_COPILOT.APPROVALS t
                USING (SELECT %(rec_id)s AS rec_id) s
                ON t.rec_id = s.rec_id
                WHEN MATCHED THEN UPDATE SET
                  approved = %(approved)s,
                  approved_by = COALESCE(%(approved_by)s, CURRENT_USER()),
                  approved_at = CURRENT_TIMESTAMP(),
                  approval_note = %(approval_note)s,
                  updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (rec_id, approved, approved_by, approved_at, approval_note, updated_at)
                VALUES (%(rec_id)s, %(approved)s, COALESCE(%(approved_by)s, CURRENT_USER()), CURRENT_TIMESTAMP(), %(approval_note)s, CURRENT_TIMESTAMP())
                """,
                {
                    "rec_id": req.rec_id,
                    "approved": req.approved,
                    "approved_by": req.approved_by,
                    "approval_note": req.approval_note,
                },
            )
            conn.commit()
    finally:
        conn.close()
    return {"status": "ok", "rec_id": req.rec_id}


@app.get("/api/actions")
def actions(limit: int = Query(default=25, le=200)):
    return {
        "items": _run_query_dict(
            """
            SELECT action_id, rec_id, mode, action_status, ddl_executed, rollback_sql, verification_query, executor, created_at
            FROM COST_COPILOT.ACTION_LOG
            ORDER BY created_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
    }


@app.get("/api/replay")
def replay(limit: int = Query(default=200, le=1000)):
    return {
        "items": _run_query_dict(
            """
            SELECT event_id, event_type, event_payload, created_at
            FROM COST_COPILOT.EVENT_STREAM
            ORDER BY created_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
    }


@app.get("/api/owners")
def owners():
    return {
        "owners": _run_query_dict(
            "SELECT owner_id, team_name, owner_name, owner_email, slack_channel FROM COST_COPILOT.DIM_OWNER ORDER BY team_name"
        ),
        "mapping": _run_query_dict(
            "SELECT map_id, query_tag_pattern, owner_id, priority, is_active FROM COST_COPILOT.JOB_OWNER_MAP ORDER BY priority ASC"
        ),
    }


@app.get("/api/budgets")
def budgets():
    return {
        "budgets": _run_query_dict("SELECT * FROM COST_COPILOT.BUDGETS WHERE active = TRUE ORDER BY created_at DESC"),
        "events": _run_query_dict("SELECT * FROM COST_COPILOT.BUDGET_EVENTS ORDER BY created_at DESC LIMIT 100"),
    }


@app.get("/api/verification")
def verification_api(limit: int = Query(default=50, le=300)):
    return {
        "items": _run_query_dict(
            """
            SELECT verification_id, rec_id, action_id, metric_name, pre_value, post_value, change_pct, verdict, created_at
            FROM COST_COPILOT.VERIFICATION_RESULTS
            ORDER BY created_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
    }


@app.websocket("/ws/events")
async def ws_events(websocket: WebSocket):
    await websocket.accept()
    try:
        rows = _run_query_dict(
            """
            SELECT event_id, event_type, event_payload, created_at
            FROM COST_COPILOT.EVENT_STREAM
            ORDER BY created_at DESC
            LIMIT 50
            """
        )
        for row in reversed(rows):
            await websocket.send_json(row)
        await websocket.send_json({"event_type": "ws.ready", "message": "event stream loaded"})
    finally:
        await websocket.close()
