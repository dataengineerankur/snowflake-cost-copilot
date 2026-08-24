import json
import os
import socket
from urllib import error, request

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Snowflake Cost Copilot", layout="wide")
st.markdown(
    """
    <style>
    .stApp {
      background: radial-gradient(circle at 10% 20%, #0f172a 0%, #020617 45%, #02030a 100%);
      color: #e2e8f0;
    }
    .stMarkdown, .stMarkdown p, .stMarkdown li, .stCaption, .stText {
      color: #e5e7eb !important;
    }
    [data-testid="stChatMessage"] {
      background: rgba(15, 23, 42, 0.45);
      border: 1px solid rgba(148, 163, 184, 0.22);
      border-radius: 12px;
      padding: 6px 10px;
      margin-bottom: 8px;
    }
    .hero {
      padding: 18px;
      border-radius: 14px;
      background: linear-gradient(90deg, rgba(56,189,248,0.16), rgba(139,92,246,0.18));
      border: 1px solid rgba(148,163,184,0.25);
      margin-bottom: 12px;
      box-shadow: 0 0 60px rgba(56,189,248,0.10);
      animation: glow 4s ease-in-out infinite;
    }
    @keyframes glow {
      0% { box-shadow: 0 0 30px rgba(56,189,248,0.12); }
      50% { box-shadow: 0 0 80px rgba(139,92,246,0.18); }
      100% { box-shadow: 0 0 30px rgba(56,189,248,0.12); }
    }
    .chip {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.3);
      margin-right: 8px;
      font-size: 12px;
      color: #cbd5e1;
    }
    .stButton > button {
      border-radius: 10px !important;
      border: 1px solid rgba(255,255,255,0.18) !important;
      background: linear-gradient(90deg, #06b6d4 0%, #8b5cf6 50%, #f59e0b 100%) !important;
      color: #ffffff !important;
      font-weight: 700 !important;
      box-shadow: 0 8px 24px rgba(6,182,212,0.25) !important;
      transition: all 0.2s ease-in-out !important;
    }
    .stButton > button:hover {
      transform: translateY(-1px) scale(1.01) !important;
      filter: brightness(1.08);
    }
    .stButton > button:disabled {
      opacity: 0.75 !important;
    }
    .welcome {
      text-align:center;
      padding: 18px 10px 10px 10px;
    }
    .welcome h2 {
      margin-bottom: 6px;
      font-size: 48px;
      font-weight: 700;
      color: #f8fafc;
      letter-spacing: -0.02em;
    }
    .welcome p {
      color: #94a3b8;
      margin-top: 0px;
    }
    .status-pill {
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      font-weight: 700;
      display: inline-block;
      margin-right: 8px;
      margin-bottom: 8px;
    }
    .status-ok { background: rgba(16,185,129,0.22); color: #a7f3d0; border: 1px solid rgba(16,185,129,0.45); }
    .status-warn { background: rgba(245,158,11,0.20); color: #fde68a; border: 1px solid rgba(245,158,11,0.45); }
    .status-err { background: rgba(239,68,68,0.20); color: #fecaca; border: 1px solid rgba(239,68,68,0.45); }
    .report-card {
      border-radius: 10px;
      border: 1px solid rgba(56,189,248,0.35);
      background: rgba(15, 23, 42, 0.65);
      color: #f8fafc;
      padding: 10px 12px;
      margin-bottom: 10px;
      line-height: 1.5;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    """
    <div class="hero">
      <h1 style="margin:0;">Snowflake Cost Copilot</h1>
      <p style="margin:6px 0 0 0;">Cinematic FinOps assistant for anomaly triage, root-cause evidence, and safe remediation.</p>
      <div style="margin-top:8px;">
        <span class="chip">Anomaly Aware</span>
        <span class="chip">Grounded AI</span>
        <span class="chip">Policy-Gated Apply</span>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

api_url = os.environ.get("CHAT_API_URL", "http://127.0.0.1:8000/chat")
api_base = api_url.replace("/chat", "")

if "conversations" not in st.session_state:
    st.session_state.conversations = {"Chat 1": []}
if "active_chat" not in st.session_state:
    st.session_state.active_chat = "Chat 1"
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = ""

st.sidebar.markdown("### ChatterStack")
st.sidebar.caption("AI Assistant")
if st.sidebar.button("+ New Chat", use_container_width=True):
    new_name = f"Chat {len(st.session_state.conversations) + 1}"
    st.session_state.conversations[new_name] = []
    st.session_state.active_chat = new_name
    st.session_state.pending_prompt = ""
    st.rerun()

chat_names = list(st.session_state.conversations.keys())
selected_chat = st.sidebar.selectbox("Conversation", options=chat_names, index=chat_names.index(st.session_state.active_chat))
st.session_state.active_chat = selected_chat

st.sidebar.markdown("---")
st.sidebar.header("Controls")
time_range = st.sidebar.selectbox("Time range", options=["24h", "7d", "30d"], index=1)
cinematic_mode = st.sidebar.toggle("Cinematic mode", value=True)
bucket = st.sidebar.selectbox("Trend bucket", options=["daily", "weekly"], index=0)
chat_mode = st.sidebar.selectbox("Chat quality", options=["Smart (LLM)", "Fast (deterministic)"], index=0)
llm_mode = "on" if chat_mode.startswith("Smart") else "off"
llm_provider = st.sidebar.selectbox("LLM provider", options=["openrouter", "groq", "cursor"], index=0)
apply_mode = st.sidebar.selectbox("Apply mode", options=["DRY_RUN", "APPLY"], index=0)
auto_create_pr = st.sidebar.toggle("Auto create PR", value=True)

with st.sidebar.expander("LLM settings & test", expanded=False):
    override_key = st.text_input("API key override (optional)", type="password")
    override_model = st.text_input("Model override (optional)")
    override_base = st.text_input("Base URL override (optional)")
    if st.button("Test LLM connection", use_container_width=True):
        payload = {
            "provider": llm_provider,
            "api_key": override_key or None,
            "model": override_model or None,
            "base_url": override_base or None,
        }
        req = request.Request(
            f"{api_base}/api/llm/test",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=25) as resp:
                test_payload = json.loads(resp.read().decode("utf-8"))
            if test_payload.get("ok"):
                st.success(f"LLM connection ok ({test_payload.get('provider')})")
            else:
                st.error(f"LLM connection failed: {test_payload}")
        except Exception as exc:
            st.error(f"LLM connection test failed: {exc}")

with st.sidebar.expander("RAG settings", expanded=False):
    rag_range = st.selectbox("RAG range", options=["7d", "30d"], index=1)
    rag_rows = st.slider("Rows/source", min_value=50, max_value=500, value=200, step=50)
    if st.button("Reindex RAG context", use_container_width=True):
        try:
            req = request.Request(
                f"{api_base}/api/rag/reindex",
                data=json.dumps({"time_range": rag_range, "max_rows_per_source": rag_rows}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=120) as resp:
                out = json.loads(resp.read().decode("utf-8"))
            st.success(f"RAG indexed: {out.get('inserted_chunks', 0)} chunks")
        except Exception as exc:
            st.error(f"RAG reindex failed: {exc}")
    if st.button("Check RAG status", use_container_width=True):
        try:
            req = request.Request(f"{api_base}/api/rag/status", method="GET")
            with request.urlopen(req, timeout=20) as resp:
                out = json.loads(resp.read().decode("utf-8"))
            st.json(out)
        except Exception as exc:
            st.error(f"RAG status failed: {exc}")

quick = st.sidebar.radio(
    "Quick prompts",
    options=[
        "Top Jobs",
        "Warehouses",
        "Pipes",
        "Recommendations",
    ],
)

quick_prompt_map = {
    "Top Jobs": "Top 5 expensive query tags last 7 days",
    "Warehouses": "Which warehouses have idle burn?",
    "Pipes": "Show ingestion failures and small-file patterns",
    "Recommendations": "Show top recommendations this week",
}

def call_api(question: str, tr: str):
    chat_history = st.session_state.conversations[st.session_state.active_chat]
    compact_history = []
    for turn in chat_history[-12:]:
        compact_history.append({"role": "user", "content": str(turn.get("question", ""))})
        compact_history.append({"role": "assistant", "content": str(turn.get("response", {}).get("summary", ""))})
    payload = {"question": question, "time_range": tr, "history": compact_history, "llm_mode": llm_mode, "llm_provider": llm_provider}
    req = request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (error.URLError, socket.timeout, TimeoutError, Exception) as exc:
        # Graceful fallback: retry deterministic path so user still gets an answer.
        try:
            payload["llm_mode"] = "off"
            retry_req = request.Request(
                api_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(retry_req, timeout=25) as resp:
                out = json.loads(resp.read().decode("utf-8"))
                assumptions = list(out.get("assumptions") or [])
                assumptions.append(f"LLM timed out; deterministic fallback returned ({exc}).")
                out["assumptions"] = assumptions
                return out
        except Exception:
            return {"summary": f"API request failed: {exc}", "top_offenders": [], "recommended_actions": [], "sql_snippets": []}

@st.cache_data(ttl=30, show_spinner=False)
def call_dashboard_api_cached(base: str, tr: str, bucket_mode: str):
    url = f"{base}/api/dashboard?range={tr}&bucket={bucket_mode}"
    req = request.Request(url, method="GET")
    with request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def call_dashboard_api(tr: str, bucket_mode: str):
    url = f"{api_base}/api/dashboard?range={tr}&bucket={bucket_mode}"
    req = request.Request(url, method="GET")
    try:
        return call_dashboard_api_cached(api_base, tr, bucket_mode)
    except (error.URLError, socket.timeout, TimeoutError, Exception) as exc:
        return {
            "summary": {"error": str(exc), "total_credits": 0, "elapsed_hours": 0, "scanned_tb": 0, "query_count": 0},
            "trend": [],
            "top_jobs": [],
            "plain_english_report": f"Dashboard unavailable: {exc}",
        }


def call_recommendations(limit: int = 20):
    try:
        req = request.Request(f"{api_base}/api/recommendations?limit={limit}", method="GET")
        with request.urlopen(req, timeout=12) as resp:
            return json.loads(resp.read().decode("utf-8")).get("items", [])
    except Exception:
        return []


@st.cache_data(ttl=15, show_spinner=False)
def get_provider_status(base: str):
    req = request.Request(f"{base}/api/llm/providers", method="GET")
    with request.urlopen(req, timeout=8) as resp:
        return json.loads(resp.read().decode("utf-8"))


def apply_recommendation(rec_id: str):
    payload = {
        "mode": apply_mode,
        "provider": llm_provider,
        "api_key": override_key or None,
        "model": override_model or None,
        "base_url": override_base or None,
        "auto_create_pr": auto_create_pr,
    }
    req = request.Request(
        f"{api_base}/api/recommendations/{rec_id}/apply_and_pr",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))

chat_tab, dashboard_tab = st.tabs(["Chat", "Dashboard"])

with chat_tab:
    provider_status = {}
    provider_error = None
    try:
        provider_status = get_provider_status(api_base)
    except Exception as exc:
        provider_error = str(exc)

    st.markdown(
        """
        <div class="welcome">
          <h2>What's on your mind today?</h2>
          <p>Ask anything about costs, spikes, jobs, or optimization opportunities.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    configured_key = f"{llm_provider}_configured"
    configured = bool(provider_status.get(configured_key)) if provider_status else False
    if llm_mode == "off":
        st.markdown('<span class="status-pill status-warn">LLM disabled (Fast mode)</span>', unsafe_allow_html=True)
    elif llm_provider == "cursor":
        st.markdown('<span class="status-pill status-warn">Cursor is remediation-only (chat uses fallback)</span>', unsafe_allow_html=True)
        st.caption("For chat answers, choose OpenRouter or Groq as provider. Cursor is used for apply-and-PR agent flow.")
    elif provider_error:
        st.markdown('<span class="status-pill status-err">LLM status unavailable</span>', unsafe_allow_html=True)
        st.caption(f"Provider status check failed: {provider_error}")
    elif configured:
        st.markdown(f'<span class="status-pill status-ok">LLM ready: {llm_provider}</span>', unsafe_allow_html=True)
    else:
        st.markdown(f'<span class="status-pill status-err">LLM key missing: {llm_provider}</span>', unsafe_allow_html=True)
        st.caption(f"Set {llm_provider.upper()}_API_KEY in `.env` or use API key override in sidebar.")

    tag_col_1, tag_col_2, tag_col_3, tag_col_4 = st.columns(4)
    if tag_col_1.button("💰 Top paid jobs", use_container_width=True):
        response = call_api("Top 5 highest paid jobs", time_range)
        st.session_state.conversations[st.session_state.active_chat].append({"question": "Top 5 highest paid jobs", "response": response})
    if tag_col_2.button("⚡ Best performing jobs", use_container_width=True):
        response = call_api("Best running jobs by efficiency", time_range)
        st.session_state.conversations[st.session_state.active_chat].append({"question": "Best running jobs by efficiency", "response": response})
    if tag_col_3.button("💧 Spill-heavy jobs", use_container_width=True):
        response = call_api("Show me jobs with highest spill", time_range)
        st.session_state.conversations[st.session_state.active_chat].append({"question": "Show me jobs with highest spill", "response": response})
    if tag_col_4.button("🧊 Idle warehouses", use_container_width=True):
        response = call_api("Which warehouses have idle burn", time_range)
        st.session_state.conversations[st.session_state.active_chat].append({"question": "Which warehouses have idle burn", "response": response})

    visible_items = st.session_state.conversations[st.session_state.active_chat][-20:]
    for msg_idx, item in enumerate(visible_items):
        with st.chat_message("user"):
            st.markdown(item["question"])
        r = item["response"]
        with st.chat_message("assistant"):
            st.markdown(r.get("summary", ""))
            st.caption(r.get("grounding_note", ""))
            if r.get("intent_mode"):
                st.caption(f"Intent mode: {r.get('intent_mode')} | Answer mode: {r.get('answer_mode')}")
            with st.expander("View details"):
                offenders = r.get("top_offenders", [])
                if offenders:
                    st.markdown("**Top offenders / metrics**")
                    df = pd.DataFrame(offenders).head(5)
                    st.dataframe(df, use_container_width=True)

                evidence_refs = r.get("evidence_references", [])
                if evidence_refs:
                    st.markdown("**Evidence references**")
                    st.code(", ".join(evidence_refs), language="text")

                actions = r.get("recommended_actions", [])
                if actions:
                    st.markdown("**Recommended actions**")
                    st.dataframe(pd.DataFrame(actions), use_container_width=True)
                    rec_ids = [a.get("REC_ID") or a.get("rec_id") for a in actions if a.get("REC_ID") or a.get("rec_id")]
                    if rec_ids:
                        selected_chat_rec = st.selectbox(
                            "Pick recommendation to apply",
                            options=rec_ids,
                            key=f"chat_apply_select_{msg_idx}",
                        )
                        if st.button("Apply this recommendation", key=f"chat_apply_btn_{msg_idx}", use_container_width=True):
                            with st.spinner("Applying remediation and preparing PR..."):
                                try:
                                    result = apply_recommendation(selected_chat_rec)
                                    if result.get("ok"):
                                        st.success("Remediation workflow completed.")
                                    else:
                                        st.error(f"Remediation workflow failed: {result.get('error') or result}")
                                    if result.get("pr_url"):
                                        st.markdown(f"PR: {result['pr_url']}")
                                except Exception as exc:
                                    st.error(f"Remediation API call failed: {exc}")

                key_metrics = r.get("key_metrics", {})
                if key_metrics:
                    k1, k2 = st.columns(2)
                    with k1:
                        st.metric("Rows returned", key_metrics.get("rows_returned", 0))
                    with k2:
                        st.metric("Top object", key_metrics.get("top_object") or "N/A")

                agent_methodology = r.get("agent_methodology", {})
                if agent_methodology:
                    st.markdown("**Agent methodology**")
                    st.write(f"Pipeline: {', '.join(agent_methodology.get('pipeline', []))}")
                    st.write(f"RAG chunks used: {agent_methodology.get('used_rag_chunks', 0)}")

                assumptions = r.get("assumptions", [])
                if assumptions:
                    st.markdown("**Assumptions**")
                    for a in assumptions:
                        st.write(f"- {a}")
                    diag = r.get("llm_diagnostics", {})
                    reason = diag.get("reason")
                    if reason and reason != "OK" and not str(reason).endswith("_OK"):
                        st.warning(f"LLM fallback reason: {reason}")

                follow_ups = r.get("follow_up_suggestions", [])
                if follow_ups:
                    st.markdown("**Follow-up ideas**")
                    for s in follow_ups:
                        st.write(f"- {s}")

                snippets = r.get("sql_snippets", [])
                if snippets:
                    st.markdown("**SQL you can run in Snowflake**")
                    for snippet in snippets:
                        st.code(snippet, language="sql")

    prompt = st.chat_input("Message AI chat...")
    if st.session_state.pending_prompt and not prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = ""
    if prompt:
        with st.spinner("Thinking..."):
            response = call_api(prompt, time_range)
        st.session_state.conversations[st.session_state.active_chat].append({"question": str(prompt), "response": response})
        st.rerun()

with dashboard_tab:
    with st.spinner("Loading dashboard..."):
        dashboard_payload = call_dashboard_api(time_range, bucket)
    summary = dashboard_payload.get("summary", {})
    trend = dashboard_payload.get("trend", [])
    top_jobs = dashboard_payload.get("top_jobs", [])
    report = dashboard_payload.get("plain_english_report", "")

    if summary.get("error"):
        st.warning(f"Dashboard API issue: {summary.get('error')}")
    st.subheader("FinOps Dashboard")
    if report:
        st.markdown(f'<div class="report-card">{report}</div>', unsafe_allow_html=True)
    dc1, dc2, dc3, dc4 = st.columns(4)
    dc1.metric("Total Credits", f"{float(summary.get('total_credits', 0) or 0):,.2f}")
    dc2.metric("Elapsed Hours", f"{float(summary.get('elapsed_hours', 0) or 0):,.1f}")
    dc3.metric("Scanned TB", f"{float(summary.get('scanned_tb', 0) or 0):,.2f}")
    dc4.metric("Query Count", f"{int(summary.get('query_count', 0) or 0):,}")

    if trend:
        tdf = pd.DataFrame(trend)
        if "credits" in tdf.columns:
            st.markdown("**Credits Trend**")
            st.line_chart(tdf.set_index("ts_bucket")[["credits"]], use_container_width=True)

    if top_jobs:
        st.markdown("**Top Highest Paid Jobs**")
        jobs_df = pd.DataFrame(top_jobs)
        st.dataframe(jobs_df, use_container_width=True)
        if cinematic_mode and "credits_total" in jobs_df.columns:
            chart_df = jobs_df[["query_tag", "credits_total"]].copy()
            chart_df = chart_df.set_index("query_tag")
            st.bar_chart(chart_df, use_container_width=True)

    st.markdown("**Recommendations (Apply + PR)**")
    recs = call_recommendations(limit=20)
    if recs:
        rec_df = pd.DataFrame(recs)
        st.dataframe(rec_df, use_container_width=True)
        rec_options = [r.get("rec_id") for r in recs if r.get("rec_id")]
        if rec_options:
            selected_rec = st.selectbox("Select recommendation to apply", options=rec_options)
            if st.button("Click to apply and create PR", use_container_width=True):
                with st.spinner("Applying remediation and preparing PR..."):
                    try:
                        result = apply_recommendation(selected_rec)
                        if result.get("ok"):
                            st.success("Remediation workflow completed.")
                        else:
                            st.error(f"Remediation workflow failed: {result.get('error') or result}")
                        if result.get("pr_url"):
                            st.markdown(f"PR: {result['pr_url']}")
                        st.json(result)
                    except Exception as exc:
                        st.error(f"Remediation API call failed: {exc}")
    else:
        st.caption("No recommendations available yet. Run ingestion/rules to generate recommendations.")
