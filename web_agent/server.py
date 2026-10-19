#!/usr/bin/env python3
"""
web_agent/server.py — Full OTel Gemini chat agent → Elastic via OTLP

ALL signals flow via OpenTelemetry SDK → OTLP/HTTP → Elastic Cloud:
  📡 Traces  → traces-generic.otel-default  (agentic waterfall: agent → tools → LLM)
  📋 Logs    → logs-generic.otel-default    (structured, trace-correlated)
  📊 Metrics → metrics-generic.otel-default (gen_ai.*, guardrails, feedback, host)

Demo features:
  ✅ Gemini function calling  → real tool call child spans in APM waterfall
  ✅ A/B model routing        → per-session, visible in metrics/traces
  ✅ PII guardrail detection  → span events + counter
  ✅ User feedback loop       → 👍/👎 → OTel log correlated to trace_id
  ✅ psutil real host metrics → CPU, memory, network observable gauges
  ✅ 100% sampling (AlwaysOn) → every turn visible in APM
"""

import os, sys, re, time, json, uuid, threading, socket, logging, itertools
from datetime import datetime, timezone
from urllib.parse import urlsplit
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

try:
    import psutil
    _PSUTIL = True
except ImportError:
    _PSUTIL = False

import httpx
from google import genai
from google.genai import types

# ── OpenTelemetry ──────────────────────────────────────────────────────────────
from opentelemetry import trace, metrics, propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggingHandler
from opentelemetry.metrics import Observation

# ── Config ────────────────────────────────────────────────────────────────────

GEMINI_KEY       = os.environ["GEMINI_API_KEY"]
OTEL_ENDPOINT    = os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"]
ES_API_KEY       = os.environ["ES_API_KEY"]
KB_ENDPOINT      = os.environ.get("KIBANA_ENDPOINT", "")  # optional — only used for "open in Kibana" links

def _apm_url(path: str) -> str | None:
    """Build a Kibana APM deep link, or None if KIBANA_ENDPOINT wasn't set."""
    return f"{KB_ENDPOINT}{path}" if KB_ENDPOINT else None

# Round-robin model pool — each new session gets the next model in order
# (not random A/B weighting). Verified live against the Gemini API before use.
MODEL_POOL = [m.strip() for m in os.environ.get(
    "GEMINI_MODEL_POOL",
    "gemini-3-flash-preview,gemini-3.1-flash-lite,gemini-3.5-flash,gemini-3.5-flash-lite",
).split(",") if m.strip()]
_rr_counter = itertools.count()  # protected by _lock at the one call site that uses it

SVC_NAME  = os.environ.get("SVC_NAME", "gemini-demo-agent")

# OTel GenAI semconv: registered gen_ai.provider.name value for the
# Gemini Developer API (generativelanguage.googleapis.com), distinct from
# "gcp.vertex_ai" (aiplatform.googleapis.com).
GENAI_PROVIDER = "gcp.gemini"
HOST_NAME = os.environ.get("HOST_NAME", socket.gethostname())
PORT      = int(os.environ.get("PORT", "5601"))
METRICS_INTERVAL_S = int(os.environ.get("METRICS_INTERVAL", "30"))

# OTLP endpoints — Elastic's own managed OTLP endpoint (OTEL_EXPORTER_OTLP_ENDPOINT),
# not guessed by string-substituting the Elasticsearch endpoint. Full paths
# required (the base URL alone causes a silent 404).
OTLP_TRACES_EP  = f"{OTEL_ENDPOINT}/v1/traces"
OTLP_METRICS_EP = f"{OTEL_ENDPOINT}/v1/metrics"
OTLP_LOGS_EP    = f"{OTEL_ENDPOINT}/v1/logs"
OTLP_HEADERS    = {"Authorization": f"ApiKey {ES_API_KEY}"}

# MCP server (mcp_server.py) — get_stock_price / get_company_news / get_analyst_rating
# run there instead of in-process, reached over real MCP JSON-RPC (Streamable HTTP).
MCP_SERVER_URL  = os.environ.get("MCP_SERVER_URL", "http://localhost:5603/mcp")
MCP_TOOL_NAMES  = {"get_stock_price", "get_company_news", "get_analyst_rating"}
MCP_PROTOCOL_VERSION = "2025-06-18"

# Skills: packaged, local formatting/synthesis capabilities — distinct from tools
# (which fetch or compute external data). Tagged with a "skill." peer.service
# prefix (our own dashboard convention, same idea as "tool.<name>") rather than
# "tool." so they group separately in APM dependency views.
SKILL_TOOL_NAMES = {"format_investor_briefing"}
_mcp_url_parts  = urlsplit(MCP_SERVER_URL)
MCP_SERVER_ADDRESS = _mcp_url_parts.hostname
MCP_SERVER_PORT     = _mcp_url_parts.port or (443 if _mcp_url_parts.scheme == "https" else 80)

SYSTEM_PROMPT = (
    "You are a helpful AI assistant. You have access to tools — use them when they "
    "would help you give a better, more accurate answer. Always search before answering "
    "factual questions. Keep responses concise (under 300 words) and use markdown. "
    "When asked for a stock briefing, report, or summary covering more than one of "
    "price/rating/news for a company, gather the relevant data first with "
    "get_stock_price/get_analyst_rating/get_company_news, then call the "
    "format_investor_briefing skill to produce the final formatted output before replying."
)

# ── OTel Resource ─────────────────────────────────────────────────────────────

resource = Resource.create({
    "service.name":            SVC_NAME,
    "service.version":         "2.0.0",
    "deployment.environment":  "demo",
    "service.instance.id":     HOST_NAME,
    "host.name":               HOST_NAME,
    "host.hostname":           HOST_NAME,
    "host.arch":               "arm64" if "arm" in os.uname().machine else "x86_64",
    "os.type":                 "darwin" if sys.platform == "darwin" else "linux",
    "process.pid":             os.getpid(),
    "process.executable.name": "python3",
    "telemetry.sdk.name":      "opentelemetry",
    "telemetry.sdk.language":  "python",
    "telemetry.distro.name":   "elastic",
})

# ── Traces ────────────────────────────────────────────────────────────────────

trace_exporter = OTLPSpanExporter(endpoint=OTLP_TRACES_EP, headers=OTLP_HEADERS)
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(SimpleSpanProcessor(trace_exporter))
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(SVC_NAME, "2.0.0")

# ── Metrics ───────────────────────────────────────────────────────────────────

metric_exporter = OTLPMetricExporter(endpoint=OTLP_METRICS_EP, headers=OTLP_HEADERS)
metric_reader   = PeriodicExportingMetricReader(
    metric_exporter, export_interval_millis=METRICS_INTERVAL_S * 1000)
meter_provider  = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(SVC_NAME, "2.0.0")

# OTel GenAI semconv instruments — https://github.com/open-telemetry/semantic-conventions-genai
# gen_ai.client.token.usage is specified as a Histogram, not a Counter.
token_usage = meter.create_histogram(
    "gen_ai.client.token.usage", unit="{token}",
    description="Number of input and output tokens used")
op_duration = meter.create_histogram(
    "gen_ai.client.operation.duration", unit="s",
    description="GenAI operation duration")

# gen_ai.execute_tool.duration is the spec's per-tool-call metric; the
# implicit histogram count (faceted by gen_ai.tool.name) gives call volume,
# so no separate "tool calls" counter is needed at this level.
tool_duration = meter.create_histogram(
    "gen_ai.execute_tool.duration", unit="s",
    description="The duration of a single tool execution")

# mcp.client.operation.duration — OTel MCP semconv, recorded only for tool
# calls actually dispatched to the mcp_server.py process over MCP.
mcp_op_duration = meter.create_histogram(
    "mcp.client.operation.duration", unit="s",
    description="The duration of the MCP request as observed on the sender")

# gen_ai.invoke_agent.tool_calls is the spec's agent-level metric: recorded
# once per invoke_agent invocation with the total tool-call count for that turn.
agent_tool_calls = meter.create_histogram(
    "gen_ai.invoke_agent.tool_calls", unit="{tool_call}",
    description="The number of tool calls a GenAI agent makes during a single invocation")

# Not part of GenAI semconv (no registered error-count metric — error rate is
# derived from error.type on gen_ai.client.operation.duration instead) or the
# guardrail/feedback signals below, so these stay outside the gen_ai.* namespace.
error_counter = meter.create_counter(
    "client.errors", unit="{error}",
    description="Errors from the generative AI client")

# Guardrail instruments
guardrail_counter = meter.create_counter(
    "guardrail.violations", unit="{violation}",
    description="Guardrail violations detected (PII, policy, etc.)")

# Feedback instruments
feedback_counter = meter.create_counter(
    "feedback.responses", unit="{response}",
    description="User feedback on agent responses (positive/negative)")

# System / process observable instruments (psutil callbacks)
if _PSUTIL:
    _cpu_pct = _mem_pct = _proc_cpu = 0.0
    _mem_bytes = _proc_mem = _net_in = _net_out = 0

    def _cb_cpu_util(opts):  yield Observation(_cpu_pct, {"system.cpu.state": "user"})
    def _cb_mem_util(opts):  yield Observation(_mem_pct, {})
    def _cb_mem_used(opts):  yield Observation(_mem_bytes, {})
    def _cb_proc_cpu(opts):  yield Observation(_proc_cpu, {"process.pid": str(os.getpid())})
    def _cb_proc_mem(opts):  yield Observation(_proc_mem, {"process.pid": str(os.getpid())})
    def _cb_net_in(opts):    yield Observation(_net_in,  {"network.io.direction": "receive"})
    def _cb_net_out(opts):   yield Observation(_net_out, {"network.io.direction": "transmit"})

    meter.create_observable_gauge("system.cpu.utilization",   callbacks=[_cb_cpu_util], unit="1")
    meter.create_observable_gauge("system.memory.utilization",callbacks=[_cb_mem_util], unit="1")
    meter.create_observable_up_down_counter("system.memory.usage",  callbacks=[_cb_mem_used], unit="By")
    meter.create_observable_gauge("process.cpu.utilization",  callbacks=[_cb_proc_cpu], unit="1")
    meter.create_observable_up_down_counter("process.memory.usage", callbacks=[_cb_proc_mem], unit="By")
    meter.create_observable_counter("system.network.io",
        callbacks=[_cb_net_in, _cb_net_out], unit="By")

# ── Logs ──────────────────────────────────────────────────────────────────────

log_exporter = OTLPLogExporter(endpoint=OTLP_LOGS_EP, headers=OTLP_HEADERS)
log_provider = LoggerProvider(resource=resource)
log_provider.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))

otel_handler = LoggingHandler(level=logging.DEBUG, logger_provider=log_provider)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(SVC_NAME)
logger.addHandler(otel_handler)
logger.propagate = False

# ── Gemini client ─────────────────────────────────────────────────────────────

GEMINI_TIMEOUT_MS = int(os.environ.get("GEMINI_TIMEOUT_MS", "30000"))  # per-request, not per-turn
gemini = genai.Client(
    api_key=GEMINI_KEY,
    http_options=types.HttpOptions(timeout=GEMINI_TIMEOUT_MS),
)

# ── MCP client (mcp_server.py) ─────────────────────────────────────────────────

mcp_http = httpx.Client(timeout=15)
_mcp_request_id = itertools.count(1)
MCP_SERVER_NAME = None  # learned via the "initialize" handshake — see _init_mcp_server()


def _init_mcp_server():
    """MCP 'initialize' handshake — learns the peer's own name (serverInfo.name)
    so it can be attached to every MCP-backed execute_tool span, instead of
    guessing it from server.address/port. Non-fatal: local tools still work
    if the MCP server isn't up yet."""
    global MCP_SERVER_NAME
    try:
        resp = mcp_http.post(MCP_SERVER_URL, json={
            "jsonrpc": "2.0", "id": next(_mcp_request_id), "method": "initialize",
            "params": {"protocolVersion": MCP_PROTOCOL_VERSION, "capabilities": {},
                       "clientInfo": {"name": SVC_NAME, "version": "2.0.0"}},
        })
        MCP_SERVER_NAME = resp.json()["result"]["serverInfo"]["name"]
        logger.info(f"MCP server initialized: {MCP_SERVER_NAME} @ {MCP_SERVER_URL}")
    except Exception as e:
        logger.warning(f"MCP server initialize failed ({e}) — "
                        f"MCP-backed tools will error until {MCP_SERVER_URL} is reachable")


_init_mcp_server()

# ── Gemini Tools (function declarations) ──────────────────────────────────────
# Each tool call in the agentic loop becomes a child span in the APM waterfall.

GEMINI_TOOLS = types.Tool(function_declarations=[
    types.FunctionDeclaration(
        name="search_knowledge_base",
        description=(
            "Search the knowledge base, documentation, or web for relevant information "
            "on any topic. Use this before answering factual or technical questions."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "query": types.Schema(
                    type=types.Type.STRING,
                    description="The search query to look up"
                )
            },
            required=["query"],
        ),
    ),
    types.FunctionDeclaration(
        name="calculate",
        description=(
            "Evaluate a mathematical expression or perform a calculation. "
            "Use for any arithmetic, unit conversion, or numeric computation."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "expression": types.Schema(
                    type=types.Type.STRING,
                    description="Mathematical expression to evaluate, e.g. '2 ** 10' or '(100 * 1.08) / 12'"
                )
            },
            required=["expression"],
        ),
    ),
    types.FunctionDeclaration(
        name="get_stock_price",
        description=(
            "Look up the current or recent price and basic info for a stock ticker symbol. "
            "Returns a company_id needed by get_company_news and get_analyst_rating — "
            "call this first if you need either of those."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "ticker": types.Schema(
                    type=types.Type.STRING,
                    description="Stock ticker symbol, e.g. ESTC, GOOG, AAPL"
                )
            },
            required=["ticker"],
        ),
    ),
    types.FunctionDeclaration(
        name="get_company_news",
        description=(
            "Get recent news headlines for a company. Requires the company_id returned "
            "by get_stock_price — you cannot call this with just a ticker symbol."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "company_id": types.Schema(
                    type=types.Type.STRING,
                    description="The company_id returned by a prior get_stock_price call, e.g. CID-AAPL-4471"
                )
            },
            required=["company_id"],
        ),
    ),
    types.FunctionDeclaration(
        name="get_analyst_rating",
        description=(
            "Get the current Wall Street analyst consensus rating (buy/hold/sell) and price "
            "target for a company. Requires the company_id returned by get_stock_price — "
            "you cannot call this with just a ticker symbol."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "company_id": types.Schema(
                    type=types.Type.STRING,
                    description="The company_id returned by a prior get_stock_price call, e.g. CID-AAPL-4471"
                )
            },
            required=["company_id"],
        ),
    ),
    types.FunctionDeclaration(
        name="format_investor_briefing",
        description=(
            "Skill (not a data-fetching tool): formats stock price, analyst rating, and "
            "news information you have ALREADY gathered into a polished, structured investor "
            "briefing. Call get_stock_price / get_analyst_rating / get_company_news first to "
            "collect the raw information, then call this to produce the final formatted output "
            "— this does not fetch or compute any new data itself, it only formats what you pass in."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "ticker": types.Schema(
                    type=types.Type.STRING,
                    description="Stock ticker symbol the briefing is about"
                ),
                "price_info": types.Schema(
                    type=types.Type.STRING,
                    description="Price/change info already gathered from get_stock_price"
                ),
                "rating_info": types.Schema(
                    type=types.Type.STRING,
                    description="Analyst rating info already gathered from get_analyst_rating"
                ),
                "news_info": types.Schema(
                    type=types.Type.STRING,
                    description="News headlines already gathered from get_company_news"
                ),
            },
            required=["ticker"],
        ),
    ),
])

def _execute_tool(name: str, args: dict) -> str:
    """Execute a local, in-process tool call and return its result string.

    get_stock_price / get_company_news / get_analyst_rating live in mcp_server.py
    instead — see _call_mcp_tool below.
    """
    if name == "search_knowledge_base":
        query = args.get("query", "")
        time.sleep(0.08)  # simulate network round-trip
        snippets = [
            f"[Doc 1] Overview of {query}: This topic covers fundamental concepts and best practices used in production systems.",
            f"[Doc 2] Advanced {query} techniques: Performance optimization and scaling strategies for enterprise deployments.",
            f"[Doc 3] {query} troubleshooting guide: Common issues, diagnostics, and resolution steps.",
        ]
        return f"Found 3 results for '{query}':\n" + "\n".join(snippets)

    elif name == "calculate":
        expr = args.get("expression", "0")
        try:
            import math
            safe_globals = {k: getattr(math, k) for k in dir(math) if not k.startswith("_")}
            result = eval(expr, {"__builtins__": {}}, safe_globals)
            return f"{expr} = {result}"
        except Exception as e:
            return f"Calculation error: {e}"

    elif name == "format_investor_briefing":
        ticker      = args.get("ticker", "").upper()
        price_info  = args.get("price_info")  or "(no price data provided)"
        rating_info = args.get("rating_info") or "(no rating data provided)"
        news_info   = args.get("news_info")   or "(no news data provided)"
        time.sleep(0.03)  # negligible — pure formatting, no I/O
        return (
            f"INVESTOR BRIEFING — {ticker}\n"
            f"{'=' * 40}\n"
            f"PRICE\n{price_info}\n\n"
            f"ANALYST RATING\n{rating_info}\n\n"
            f"RECENT NEWS\n{news_info}\n"
            f"{'=' * 40}"
        )

    return f"Unknown tool: {name}"


def _safe_args_summary(args: dict, max_value_len: int = 40) -> str:
    """JSON-encode tool call arguments for display, truncating each value —
    never the serialized blob itself — so the result is always valid JSON
    (the chat UI does JSON.parse() on this to render tool-call chips)."""
    truncated = {
        k: (v[:max_value_len] + "…" if isinstance(v, str) and len(v) > max_value_len else v)
        for k, v in args.items()
    }
    return json.dumps(truncated)


def _call_mcp_tool(name: str, args: dict) -> tuple[str, dict]:
    """Call a tool served by mcp_server.py over real MCP JSON-RPC (Streamable HTTP).

    Propagates the current trace context via the MCP request's params._meta
    (NOT an HTTP header — MCP context propagation is transport-independent, per
    https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/mcp.md),
    so mcp_server.py's SERVER span becomes a child of our currently-open
    execute_tool span, giving one continuous distributed trace.

    Returns (result_text, meta) where meta carries the MCP-specific attributes
    the caller sets on the (already open) execute_tool span.
    """
    if MCP_SERVER_NAME is None:
        _init_mcp_server()  # e.g. mcp_server.py wasn't up yet at agent startup — retry once

    meta = {}
    propagate.inject(meta)

    req_id = next(_mcp_request_id)
    request = {
        "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
        "params": {"name": name, "arguments": args, "_meta": meta},
    }

    result_meta = {
        "jsonrpc.request.id": str(req_id),
        "mcp.protocol.version": MCP_PROTOCOL_VERSION,
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "service.name": MCP_SERVER_NAME or f"unknown ({MCP_SERVER_URL})",
    }

    try:
        response = mcp_http.post(MCP_SERVER_URL, json=request)
        result_meta["network.protocol.version"] = response.http_version.replace("HTTP/", "")
        body = response.json()
    except Exception as e:
        result_meta["error.type"] = type(e).__name__
        return f"MCP transport error calling {name}: {e}", result_meta

    if "error" in body:
        err = body["error"]
        result_meta["rpc.response.status_code"] = str(err.get("code", ""))
        result_meta["error.type"] = str(err.get("code", ""))
        return f"MCP error calling {name}: {err.get('message', 'unknown error')}", result_meta

    content = body.get("result", {}).get("content", [])
    text = content[0]["text"] if content and "text" in content[0] else json.dumps(body.get("result"))
    if body.get("result", {}).get("isError"):
        result_meta["error.type"] = "tool_error"
    return text, result_meta


# ── PII detection ─────────────────────────────────────────────────────────────

_PII_PATTERNS = {
    "email":       r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    "phone":       r"\b\d{3}[\-.\s]?\d{3}[\-.\s]?\d{4}\b",
    "ssn":         r"\b\d{3}-\d{2}-\d{4}\b",
    "credit_card": r"\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b",
}

def detect_pii(text: str) -> list[str]:
    return [pii_type for pii_type, pat in _PII_PATTERNS.items()
            if re.search(pat, text, re.IGNORECASE)]


# ── psutil scrape loop ────────────────────────────────────────────────────────

def _psutil_scrape_loop():
    if not _PSUTIL:
        return
    global _cpu_pct, _mem_pct, _mem_bytes, _proc_cpu, _proc_mem, _net_in, _net_out
    proc = psutil.Process(os.getpid())
    psutil.cpu_percent(interval=None)
    net_prev = (0, 0)
    while True:
        time.sleep(METRICS_INTERVAL_S)
        try:
            _cpu_pct   = round(psutil.cpu_percent(interval=None) / 100.0, 4)
            mem        = psutil.virtual_memory()
            _mem_pct   = round(mem.percent / 100.0, 4)
            _mem_bytes = mem.used
            with proc.oneshot():
                _proc_cpu = round(proc.cpu_percent(interval=None) / 100.0, 4)
                _proc_mem = proc.memory_info().rss
            net       = psutil.net_io_counters()
            _net_in   = max(0, net.bytes_recv - net_prev[0])
            _net_out  = max(0, net.bytes_sent - net_prev[1])
            net_prev  = (net.bytes_recv, net.bytes_sent)
        except Exception:
            pass


# ── Chat turn ─────────────────────────────────────────────────────────────────

def chat_turn(user_message: str, conversation_id: str, history: list,
              model: str, variant: str) -> dict:
    """
    Full agentic turn with OTel instrumentation (OTel GenAI semconv):

      invoke_agent {SVC_NAME} [INTERNAL]                ← root span / APM transaction
        ├── execute_tool search_knowledge_base [INTERNAL]  ← tool call spans
        ├── execute_tool calculate [INTERNAL]
        └── generate_content {MODEL} [CLIENT]           ← LLM inference span

    Also emits:
      - PII guardrail check (span event + metric if triggered)
      - gen_ai.input.messages / gen_ai.output.messages span attributes
      - gen_ai.client.token.usage, gen_ai.client.operation.duration,
        gen_ai.execute_tool.duration, gen_ai.invoke_agent.tool_calls metrics
      - Structured logs correlated to trace_id
    """
    t0 = time.time()
    trace_id = tx_id = None
    tool_calls_made = []   # [{name, args_summary, result_summary, duration_ms}]

    with tracer.start_as_current_span(
        f"invoke_agent {SVC_NAME}",
        kind=SpanKind.INTERNAL,
        attributes={
            "gen_ai.operation.name":   "invoke_agent",
            "gen_ai.agent.name":       SVC_NAME,
            "gen_ai.request.model":    model,
            "gen_ai.conversation.id":  conversation_id,
            "ab.variant":              variant,
            "ab.model":                model,
        },
    ) as root:
        ctx      = root.get_span_context()
        trace_id = format(ctx.trace_id, "032x")
        tx_id    = format(ctx.span_id,  "016x")

        # ── PII guardrail ───────────────────────────────────────────────
        pii_types = detect_pii(user_message)
        if pii_types:
            root.add_event("guardrail.pii_detected", {
                "pii.types":          ", ".join(pii_types),
                "pii.message_length": len(user_message),
            })
            root.set_attribute("guardrail.pii_detected", True)
            for pii_type in pii_types:
                guardrail_counter.add(1, {
                    "guardrail.type":       "pii",
                    "pii.type":             pii_type,
                    "gen_ai.provider.name": GENAI_PROVIDER,
                })
            logger.warning("PII detected in user message",
                           extra={"pii_types": pii_types, "conversation_id": conversation_id})

        logger.info("Turn started", extra={
            "conversation_id": conversation_id,
            "model": model, "variant": variant,
            "user_message_len": len(user_message),
            "pii_detected": bool(pii_types),
        })

        # ── Agentic LLM loop with tool calls ───────────────────────────
        in_tok = out_tok = 0
        is_error = False
        error_type = None
        output_text = ""

        with tracer.start_as_current_span(
            f"generate_content {model}",
            kind=SpanKind.CLIENT,
            attributes={
                "gen_ai.operation.name":  "generate_content",
                "gen_ai.provider.name":   GENAI_PROVIDER,
                "gen_ai.request.model":   model,
                "peer.service":           "google_gemini",
                "server.address":         "generativelanguage.googleapis.com",
                "server.port":            443,
                "network.protocol.name":  "https",
                "rpc.system":             "http",
                "rpc.service":            "google.ai.generativelanguage",
                "rpc.method":             "GenerateContent",
                "ab.variant":             variant,
            },
        ) as llm:
            try:
                # gen_ai.input.messages / gen_ai.system_instructions — structured
                # per https://github.com/open-telemetry/semantic-conventions-genai
                # model/gen-ai/gen-ai-input-messages.json (recorded as a JSON
                # string on spans, per the "Recording content on attributes" note).
                input_messages = [
                    {"role": "assistant" if t["role"] == "model" else t["role"],
                     "parts": [{"type": "text", "content": t["text"]}]}
                    for t in history
                ] + [{"role": "user", "parts": [{"type": "text", "content": user_message}]}]
                llm.set_attribute("gen_ai.input.messages", json.dumps(input_messages)[:4000])
                llm.set_attribute("gen_ai.system_instructions",
                                   json.dumps([{"type": "text", "content": SYSTEM_PROMPT}]))

                # Build conversation contents
                contents = [
                    types.Content(role=t["role"], parts=[types.Part(text=t["text"])])
                    for t in history
                ] + [types.Content(role="user", parts=[types.Part(text=user_message)])]

                cfg = types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0.7,
                    max_output_tokens=512,
                    tools=[GEMINI_TOOLS],
                )

                # Agentic loop — iterate until model returns text (no more tool calls)
                for _iteration in range(6):
                    response = gemini.models.generate_content(
                        model=model, contents=contents, config=cfg)

                    # Accumulate tokens across iterations
                    usage   = response.usage_metadata
                    in_tok  += getattr(usage, "prompt_token_count",     0) or 0
                    out_tok += getattr(usage, "candidates_token_count", 0) or 0

                    # Collect function call parts
                    candidate_parts = response.candidates[0].content.parts
                    fn_calls = [p.function_call for p in candidate_parts
                                if hasattr(p, "function_call") and p.function_call]

                    if not fn_calls:
                        # Final text response
                        output_text = response.text or ""
                        break

                    # Execute each tool call — each gets its own child span
                    fn_response_parts = []
                    for fc in fn_calls:
                        tool_name = fc.name
                        tool_args = dict(fc.args) if fc.args else {}
                        is_mcp_tool = tool_name in MCP_TOOL_NAMES
                        is_skill = tool_name in SKILL_TOOL_NAMES
                        t_tool = time.time()

                        # Dependency-name prefix drives dashboard bucketing: "skill."
                        # for packaged formatting/synthesis skills, "tool." for
                        # everything else (data-fetching functions, local or MCP).
                        _dep_prefix = "skill" if is_skill else "tool"

                        with tracer.start_as_current_span(
                            f"execute_tool {tool_name}",
                            # A genuine network hop to mcp_server.py is CLIENT kind;
                            # in-process tools/skills stay INTERNAL, per the execute_tool span spec.
                            kind=SpanKind.CLIENT if is_mcp_tool else SpanKind.INTERNAL,
                            attributes={
                                "gen_ai.operation.name":  "execute_tool",
                                "gen_ai.tool.name":       tool_name,
                                "gen_ai.tool.call.id":    f"{tool_name}_{_iteration}",
                                "gen_ai.tool.type":       "function",
                                "gen_ai.agent.name":      SVC_NAME,
                                "gen_ai.conversation.id": conversation_id,
                                "gen_ai.tool.call.arguments": json.dumps(tool_args)[:400],
                                "peer.service":           f"{_dep_prefix}.{tool_name}",
                            },
                        ) as tool_span:
                            mcp_meta = None
                            if is_mcp_tool:
                                result, mcp_meta = _call_mcp_tool(tool_name, tool_args)
                            else:
                                result = _execute_tool(tool_name, tool_args)
                            tool_dur = time.time() - t_tool

                            tool_span.set_attribute("gen_ai.tool.call.result", result[:400])
                            tool_span.set_attribute("tool.result_length", len(result))

                            if mcp_meta is not None:
                                # MCP semconv: add MCP-specific attributes to the existing
                                # tool execution span rather than nesting a separate one.
                                tool_span.set_attribute("mcp.method.name", "tools/call")
                                tool_span.set_attribute("mcp.protocol.version", mcp_meta["mcp.protocol.version"])
                                tool_span.set_attribute("jsonrpc.request.id", mcp_meta["jsonrpc.request.id"])
                                tool_span.set_attribute("network.protocol.name", mcp_meta["network.protocol.name"])
                                tool_span.set_attribute("network.transport", mcp_meta["network.transport"])
                                if "network.protocol.version" in mcp_meta:
                                    tool_span.set_attribute("network.protocol.version", mcp_meta["network.protocol.version"])
                                tool_span.set_attribute("server.address", MCP_SERVER_ADDRESS)
                                tool_span.set_attribute("server.port", MCP_SERVER_PORT)
                                # service.name is normally a resource attribute; setting it on
                                # the span records the peer's identity (learned via its own
                                # "initialize" serverInfo.name) without inventing a new,
                                # unregistered mcp.* attribute. Lands as attributes.service.name
                                # in Elastic — distinct from the top-level service.name field,
                                # which stays "gemini-demo-agent" (this process's own resource).
                                tool_span.set_attribute("service.name", mcp_meta["service.name"])

                                mcp_metric_attrs = {
                                    "mcp.method.name":       "tools/call",
                                    "gen_ai.operation.name": "execute_tool",
                                    "gen_ai.tool.name":      tool_name,
                                    "network.transport":     mcp_meta["network.transport"],
                                }
                                if "error.type" in mcp_meta:
                                    tool_span.set_attribute("error.type", mcp_meta["error.type"])
                                    mcp_metric_attrs["error.type"] = mcp_meta["error.type"]
                                    if "rpc.response.status_code" in mcp_meta:
                                        tool_span.set_attribute("rpc.response.status_code", mcp_meta["rpc.response.status_code"])
                                        mcp_metric_attrs["rpc.response.status_code"] = mcp_meta["rpc.response.status_code"]
                                    tool_span.set_status(StatusCode.ERROR, result[:200])
                                else:
                                    tool_span.set_status(StatusCode.OK)
                                mcp_op_duration.record(tool_dur, mcp_metric_attrs)
                            else:
                                tool_span.set_status(StatusCode.OK)

                            # gen_ai.execute_tool.duration — matches the span duration,
                            # per the metric's spec note.
                            tool_duration.record(tool_dur, {
                                "gen_ai.tool.name":  tool_name,
                                "gen_ai.agent.name": SVC_NAME,
                            })

                            tool_calls_made.append({
                                "name":           tool_name,
                                "args_summary":   _safe_args_summary(tool_args),
                                "result_summary": result[:120],
                                "duration_ms":    round(tool_dur * 1000),
                            })

                        fn_response_parts.append(
                            types.Part.from_function_response(
                                name=tool_name, response={"result": result}
                            )
                        )

                    # Append model's function call message + our function results
                    contents.append(response.candidates[0].content)
                    contents.append(types.Content(role="user", parts=fn_response_parts))

                # gen_ai.output.messages — structured, per the output messages schema
                output_messages = [{"role": "assistant",
                                     "parts": [{"type": "text", "content": output_text}]}]
                llm.set_attribute("gen_ai.output.messages", json.dumps(output_messages)[:4000])

                llm.set_attribute("gen_ai.response.model",      model)
                llm.set_attribute("gen_ai.usage.input_tokens",  in_tok)
                llm.set_attribute("gen_ai.usage.output_tokens", out_tok)
                llm.set_attribute("tool.calls_count",           len(tool_calls_made))
                llm.set_status(StatusCode.OK)

                root.set_attribute("gen_ai.usage.input_tokens",  in_tok)
                root.set_attribute("gen_ai.usage.output_tokens", out_tok)
                root.set_attribute("tool.calls_count",           len(tool_calls_made))
                root.set_status(StatusCode.OK)

                # gen_ai.client.token.usage — Histogram per spec
                attrs = {"gen_ai.operation.name": "generate_content",
                         "gen_ai.provider.name": GENAI_PROVIDER,
                         "gen_ai.request.model": model, "ab.variant": variant}
                token_usage.record(in_tok,  {**attrs, "gen_ai.token.type": "input"})
                token_usage.record(out_tok, {**attrs, "gen_ai.token.type": "output"})

            except Exception as e:
                llm.set_status(StatusCode.ERROR, str(e))
                llm.record_exception(e)
                root.set_status(StatusCode.ERROR, str(e))
                logger.error(f"Gemini API error: {e}", exc_info=True,
                             extra={"model": model, "conversation_id": conversation_id})
                error_type = type(e).__name__
                error_counter.add(1, {"gen_ai.operation.name": "generate_content",
                                      "gen_ai.provider.name": GENAI_PROVIDER,
                                      "error.type": error_type,
                                      "ab.variant": variant})
                output_text = f"Sorry, I encountered an error: {e}"
                is_error = True

            # gen_ai.invoke_agent.tool_calls — Histogram, recorded once per
            # agent invocation with the total tool-call count for this turn.
            agent_tool_calls.record(len(tool_calls_made), {"gen_ai.agent.name": SVC_NAME})

    elapsed_s  = time.time() - t0
    cost_usd   = (in_tok * 0.075 + out_tok * 0.30) / 1_000_000

    op_duration.record(elapsed_s, {
        "gen_ai.operation.name":  "generate_content",
        "gen_ai.provider.name":   GENAI_PROVIDER,
        "gen_ai.request.model":   model,
        "gen_ai.response.model":  model,
        "ab.variant":             variant,
        **({"error.type": error_type} if is_error else {}),
    })

    logger.info("Turn completed", extra={
        "conversation_id": conversation_id,
        "model": model, "variant": variant,
        "in_tokens": in_tok, "out_tokens": out_tok,
        "latency_ms": round(elapsed_s * 1000),
        "cost_usd": round(cost_usd, 6),
        "tool_calls": len(tool_calls_made),
        "is_error": is_error,
    })

    return {
        "text":        output_text,
        "trace_id":    trace_id,
        "tx_id":       tx_id,
        "in_tokens":   in_tok,
        "out_tokens":  out_tok,
        "latency_ms":  round(elapsed_s * 1000),
        "cost_usd":    round(cost_usd, 6),
        "is_error":    is_error,
        "variant":     variant,
        "model_used":  model,
        "tool_calls":  tool_calls_made,
        "pii_detected": bool(pii_types),
        "transport":   "otlp",
        "apm_url":     _apm_url(f"/app/apm/services/{SVC_NAME}/transactions/view"
                                f"?transactionName=invoke_agent%20{SVC_NAME}&transactionType=unknown"),
    }


# ── In-memory sessions ────────────────────────────────────────────────────────

_sessions = {}
_lock     = threading.Lock()

def get_or_create_session(session_id=None):
    with _lock:
        if not session_id or session_id not in _sessions:
            session_id = uuid.uuid4().hex[:12]
            # Round-robin across MODEL_POOL — assigned once at session creation,
            # consistent within a session, cycling in order rather than randomly.
            idx     = next(_rr_counter) % len(MODEL_POOL)
            model   = MODEL_POOL[idx]
            variant = chr(ord("A") + (idx % 26))
            _sessions[session_id] = {
                "id":      session_id,
                "conv_id": uuid.uuid4().hex[:8],
                "history": [],
                "variant": variant,
                "model":   model,
            }
        return session_id, _sessions[session_id]


# ── Web UI ────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Elastic AI Agent — Full OTel Demo</title>
<style>
  :root{
    --bg:#0d1117;--surface:#161b22;--border:#30363d;
    --text:#e6edf3;--text-2:#8b949e;--text-3:#6e7681;
    --blue:#58a6ff;--green:#3fb950;--yellow:#d29922;
    --red:#f85149;--elastic:#00bfb3;--elastic-2:#003d38;--otel:#7b61ff;
    --tool:#f0883e;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;height:100vh;display:flex;flex-direction:column;overflow:hidden}

  header{background:var(--surface);border-bottom:1px solid var(--border);padding:10px 20px;display:flex;align-items:center;gap:14px;flex-shrink:0}
  .logo{font-size:17px;font-weight:700;color:var(--elastic)}
  .logo span{color:var(--text-2);font-weight:400;font-size:13px;margin-left:6px}
  .badges{display:flex;gap:8px;margin-left:auto;align-items:center}
  .badge{font-size:11px;border-radius:4px;padding:2px 8px;border:1px solid var(--border);color:var(--text-2)}
  .badge.otel{border-color:var(--otel);color:var(--otel)}
  .badge.live{border-color:var(--elastic);color:var(--elastic)}
  .badge.ab-a{border-color:var(--blue);color:var(--blue)}
  .badge.ab-b{border-color:var(--tool);color:var(--tool)}
  .badge.ab-c{border-color:var(--green);color:var(--green)}
  .badge.ab-d{border-color:var(--yellow);color:var(--yellow)}
  .badge.ab-e{border-color:var(--red);color:var(--red)}
  .badge.ab-f{border-color:var(--otel);color:var(--otel)}
  .apm-btn{color:var(--blue);text-decoration:none;font-size:12px;border:1px solid var(--border);border-radius:4px;padding:3px 10px;transition:border-color .15s}
  .apm-btn:hover{border-color:var(--blue)}

  main{display:flex;flex:1;min-height:0}

  /* Chat column */
  .chat-col{flex:1;display:flex;flex-direction:column;min-width:0}
  .messages{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:12px}
  .messages::-webkit-scrollbar{width:5px}
  .messages::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}

  .msg{display:flex;gap:10px;max-width:84%}
  .msg.user{align-self:flex-end;flex-direction:row-reverse}
  .avatar{width:30px;height:30px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0;margin-top:2px}
  .msg.user .avatar{background:#1c2d3a;border:1px solid var(--blue)}
  .msg.agent .avatar{background:#001a18;border:1px solid var(--elastic)}
  .bubble{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:10px 14px;font-size:14px;line-height:1.65;word-break:break-word}
  .msg.user .bubble{background:#1c2d3a;border-color:var(--blue)}
  .bubble pre{background:#0d1117;border:1px solid var(--border);border-radius:6px;padding:10px;overflow-x:auto;font-size:12px;margin:8px 0}
  .bubble code{background:#111;border:1px solid var(--border);border-radius:3px;padding:1px 5px;font-size:12px}
  .bubble strong{color:var(--text)}

  /* Tool call display */
  .tools-row{margin-left:40px;margin-top:-6px;display:flex;flex-direction:column;gap:4px}
  .tool-chip{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--tool);background:#1a0f00;border:1px solid #3d2000;border-radius:6px;padding:4px 10px;width:fit-content;cursor:pointer;transition:border-color .15s}
  .tool-chip:hover{border-color:var(--tool)}
  .tool-chip .tc-name{font-weight:600}
  .tool-chip .tc-dur{color:var(--text-3);margin-left:auto}
  .tool-detail{display:none;font-size:11px;font-family:monospace;color:var(--text-3);background:#0d0800;border:1px solid #2a1a00;border-radius:4px;padding:6px 10px;margin-left:40px;word-break:break-all}
  .tool-detail.open{display:block}

  /* Pills row */
  .pills-row{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-left:40px;margin-top:-4px}
  .pill{font-size:11px;border-radius:4px;padding:2px 8px;border:1px solid}
  .pill.trace{border-color:var(--elastic);color:var(--elastic);text-decoration:none;cursor:pointer}
  .pill.trace:hover{background:var(--elastic-2)}
  .pill.dim{border-color:var(--border);color:var(--text-3)}
  .pill.err{border-color:var(--red);color:var(--red)}
  .pill.otlp{border-color:var(--otel);color:var(--otel)}
  .pill.pii{border-color:var(--red);color:var(--red)}
  .pill.ab-a{border-color:var(--blue);color:var(--blue)}
  .pill.ab-b{border-color:var(--tool);color:var(--tool)}
  .pill.ab-c{border-color:var(--green);color:var(--green)}
  .pill.ab-d{border-color:var(--yellow);color:var(--yellow)}
  .pill.ab-e{border-color:var(--red);color:var(--red)}
  .pill.ab-f{border-color:var(--otel);color:var(--otel)}

  /* Feedback buttons */
  .feedback-row{display:flex;gap:8px;align-items:center;margin-left:40px;margin-top:2px}
  .fb-btn{background:transparent;border:1px solid var(--border);color:var(--text-3);border-radius:6px;padding:3px 10px;font-size:13px;cursor:pointer;transition:all .15s}
  .fb-btn:hover{border-color:var(--text-2);color:var(--text)}
  .fb-btn.voted-up{border-color:var(--green);color:var(--green);background:#0a1f10}
  .fb-btn.voted-down{border-color:var(--red);color:var(--red);background:#1a0808}
  .fb-lbl{font-size:11px;color:var(--text-3)}

  /* Typing indicator */
  .typing{display:flex;align-items:center;gap:6px;padding:10px 14px;color:var(--text-3);font-size:13px}
  .dots{display:flex;gap:4px}
  .dot{width:6px;height:6px;border-radius:50%;background:var(--elastic);animation:bop .8s infinite alternate}
  .dot:nth-child(2){animation-delay:.15s}.dot:nth-child(3){animation-delay:.3s}
  @keyframes bop{to{transform:translateY(-4px);opacity:.4}}

  .input-row{border-top:1px solid var(--border);padding:12px 20px;display:flex;gap:10px;background:var(--surface);flex-shrink:0}
  textarea{flex:1;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px 14px;font-size:14px;font-family:inherit;resize:none;height:42px;max-height:120px;line-height:1.5;transition:border-color .15s}
  textarea:focus{outline:none;border-color:var(--elastic)}
  .send-btn{background:var(--elastic);border:none;color:#000;border-radius:8px;padding:10px 20px;font-size:14px;font-weight:600;cursor:pointer;transition:opacity .15s}
  .send-btn:hover{opacity:.85}.send-btn:disabled{opacity:.35;cursor:not-allowed}
  .new-btn{background:transparent;border:1px solid var(--border);color:var(--text-2);border-radius:8px;padding:10px 14px;font-size:13px;cursor:pointer}
  .new-btn:hover{border-color:var(--text-2)}

  /* Sidebar */
  .tele{width:272px;border-left:1px solid var(--border);background:var(--surface);display:flex;flex-direction:column;overflow-y:auto;flex-shrink:0}
  .tele-hdr{padding:10px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:700;color:var(--text-2);text-transform:uppercase;letter-spacing:.6px}
  .tele-sec{padding:12px 14px;border-bottom:1px solid var(--border)}
  .tele-lbl{font-size:10px;text-transform:uppercase;color:var(--text-3);letter-spacing:.5px;margin-bottom:7px}
  .kv{display:flex;justify-content:space-between;margin-bottom:5px;font-size:12px}
  .kv span:first-child{color:var(--text-2)}
  .kv span:last-child{color:var(--text);font-weight:500;font-variant-numeric:tabular-nums}
  .kv span.g{color:var(--green)}.kv span.b{color:var(--blue)}.kv span.y{color:var(--yellow)}
  .kv span.e{color:var(--elastic)}.kv span.r{color:var(--red)}.kv span.t{color:var(--tool)}
  .mono{font-size:10px;font-family:monospace;color:var(--text-3);word-break:break-all;margin-top:3px;line-height:1.4}
  .sig-row{display:flex;align-items:center;gap:7px;margin-bottom:5px;font-size:12px}
  .dot-s{width:7px;height:7px;border-radius:50%;flex-shrink:0}
  .dot-s.ok{background:var(--green)}.dot-s.dim{background:var(--text-3)}
  .dot-s.tool{background:var(--tool)}
  .sig-txt{color:var(--text-2)}.sig-sub{color:var(--text-3);font-size:10px}

  /* Demo scenario prompt cards */
  .scenario-list{display:flex;flex-direction:column;gap:6px}
  .scenario-card{border:1px solid var(--border);border-radius:8px;padding:8px 10px;cursor:pointer;background:var(--bg);transition:border-color .15s,background .15s}
  .scenario-card:hover{border-color:var(--elastic);background:var(--elastic-2)}
  .scenario-card:active{opacity:.7}
  .scenario-top{display:flex;align-items:center;gap:6px;margin-bottom:3px}
  .scenario-title{font-size:12px;font-weight:600;color:var(--text)}
  .scenario-desc{font-size:10.5px;color:var(--text-3);line-height:1.4}
  .scenario-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
  .scenario-dot.tool{background:var(--tool)}
  .scenario-dot.mcp{background:var(--blue)}
  .scenario-dot.skill{background:var(--green)}
  .scenario-dot.pii{background:var(--red)}

  .welcome{text-align:center;padding:50px 30px;color:var(--text-2)}
  .welcome h2{color:var(--text);font-size:20px;margin-bottom:8px}
  .welcome p{font-size:13px;line-height:1.6;max-width:360px;margin:0 auto 10px}
  .welcome .tag{color:var(--elastic);font-size:12px}

  /* Data flow diagram */
  .flow{display:flex;flex-direction:column;align-items:center;gap:4px;margin-top:6px}
  .fnode{border:1px solid var(--border);border-radius:8px;padding:6px 10px;font-size:11px;text-align:center;width:100%;background:var(--bg)}
  .fnode.user-node{border-color:var(--blue);color:var(--blue)}
  .fnode.app-node{border-color:var(--elastic);color:var(--elastic)}
  .fnode.tool-node{border-color:var(--tool);color:var(--tool)}
  .fnode.llm-node{border-color:var(--otel);color:var(--otel)}
  .fnode.ingest-node{border-color:#5a3ea0;color:#a371f7}
  .fnode.kibana-node{border-color:var(--green);color:var(--green)}
  .farrow{font-size:10px;color:var(--text-3)}
  .frow3{display:flex;gap:4px;width:100%}
  .fsmall{flex:1;border:1px solid var(--border);border-radius:6px;padding:5px 3px;font-size:10px;text-align:center;background:var(--bg);color:var(--text-2)}
</style>
</head>
<body>
<header>
  <div class="logo">Elastic<span>/ AI Agent Observability</span></div>
  <div class="badges">
    <span class="badge" id="hdr-model">—</span>
    <span class="badge" id="hdr-ab">—</span>
    <span class="badge otel">OTel OTLP</span>
    <span class="badge live">● LIVE</span>
    <a id="apm-link" class="apm-btn" href="#" target="_blank">APM ↗</a>
  </div>
</header>

<main>
  <div class="chat-col">
    <div class="messages" id="msgs">
      <div class="welcome">
        <h2>Gemini AI Agent</h2>
        <p>Ask anything. The agent calls real tools (search, calculate, stock price lookup) and every interaction ships traces, logs, and metrics to Elastic via OpenTelemetry.</p>
        <p class="tag">Try: "What is vector search?" · "Calculate 2^32" · "ESTC stock price"</p>
      </div>
    </div>
    <div class="input-row">
      <button class="new-btn" id="new-btn">+ New</button>
      <textarea id="inp" placeholder="Ask anything — tools will be called automatically…"></textarea>
      <button class="send-btn" id="send-btn">Send</button>
    </div>
  </div>

  <div class="tele">
    <div class="tele-hdr">Demo Scenarios</div>

    <div class="tele-sec">
      <div class="tele-lbl">Click a prompt to run it</div>
      <div class="scenario-list">
        <div class="scenario-card" data-cat="tool" data-prompt="What are the best practices for Kubernetes autoscaling?">
          <div class="scenario-top"><span class="scenario-dot tool"></span><span class="scenario-title">Local tool — search</span></div>
          <div class="scenario-desc">In-process search_knowledge_base — no network hop, INTERNAL span.</div>
        </div>
        <div class="scenario-card" data-cat="tool" data-prompt="Calculate 20% of 350, and search for info on OpenTelemetry trace context propagation.">
          <div class="scenario-top"><span class="scenario-dot tool"></span><span class="scenario-title">Local tools — combo</span></div>
          <div class="scenario-desc">calculate + search_knowledge_base in one turn, both local.</div>
        </div>
        <div class="scenario-card" data-cat="mcp" data-prompt="What's the current stock price of AAPL?">
          <div class="scenario-top"><span class="scenario-dot mcp"></span><span class="scenario-title">MCP tool — single call</span></div>
          <div class="scenario-desc">get_stock_price served by the separate MCP server — real network hop (CLIENT→SERVER spans).</div>
        </div>
        <div class="scenario-card" data-cat="mcp" data-prompt="What's the analyst rating and recent news for MSFT?">
          <div class="scenario-top"><span class="scenario-dot mcp"></span><span class="scenario-title">MCP tool chain</span></div>
          <div class="scenario-desc">get_stock_price → get_analyst_rating + get_company_news — 3 chained MCP calls, one dependency (company_id).</div>
        </div>
        <div class="scenario-card" data-cat="skill" data-prompt="Give me a full investor briefing for MSFT including price, analyst rating, and recent news.">
          <div class="scenario-top"><span class="scenario-dot skill"></span><span class="scenario-title">Skill — investor briefing</span></div>
          <div class="scenario-desc">Gathers data via MCP, then invokes the format_investor_briefing skill to synthesize the final report.</div>
        </div>
        <div class="scenario-card" data-cat="pii" data-prompt="My email is john.doe@example.com — can you help me troubleshoot why I can't log in?">
          <div class="scenario-top"><span class="scenario-dot pii"></span><span class="scenario-title">PII guardrail</span></div>
          <div class="scenario-desc">Message contains an email address — triggers the PII detector span event + metric.</div>
        </div>
      </div>
    </div>

    <div class="tele-hdr">Live OTel Telemetry</div>

    <div class="tele-sec">
      <div class="tele-lbl">Signals via OTLP/HTTP</div>
      <div class="sig-row"><div class="dot-s ok"></div><div><div class="sig-txt">Traces</div><div class="sig-sub">traces-generic.otel-default</div></div></div>
      <div class="sig-row"><div class="dot-s ok"></div><div><div class="sig-txt">Logs</div><div class="sig-sub">logs-generic.otel-default</div></div></div>
      <div class="sig-row"><div class="dot-s ok"></div><div><div class="sig-txt">Metrics</div><div class="sig-sub">metrics-generic.otel-default</div></div></div>
      <div class="sig-row"><div class="dot-s tool"></div><div><div class="sig-txt">Tool spans</div><div class="sig-sub">child spans per tool call</div></div></div>
      <div class="sig-row"><div class="dot-s" id="psutil-dot"></div><div><div class="sig-txt">Host metrics</div><div class="sig-sub">psutil → OTel gauges</div></div></div>
    </div>

    <div class="tele-sec">
      <div class="tele-lbl">Round-Robin Models</div>
      <div class="kv"><span>This session</span><span class="b" id="t-variant">—</span></div>
      <div class="kv"><span>Model</span><span class="e" id="t-model-used">—</span></div>
      <div id="model-pool-list"></div>
    </div>

    <div class="tele-sec">
      <div class="tele-lbl">Last Turn</div>
      <div class="kv"><span>Latency</span><span class="b" id="t-lat">—</span></div>
      <div class="kv"><span>Tokens in</span><span id="t-in">—</span></div>
      <div class="kv"><span>Tokens out</span><span id="t-out">—</span></div>
      <div class="kv"><span>Cost</span><span class="y" id="t-cost">—</span></div>
      <div class="kv"><span>Tool calls</span><span class="t" id="t-tools">—</span></div>
      <div class="tele-lbl" style="margin-top:10px">Trace ID</div>
      <div class="mono" id="t-tid">—</div>
    </div>

    <div class="tele-sec">
      <div class="tele-lbl">Session Totals</div>
      <div class="kv"><span>Turns</span><span class="e" id="s-turns">0</span></div>
      <div class="kv"><span>Tokens in</span><span id="s-in">0</span></div>
      <div class="kv"><span>Tokens out</span><span id="s-out">0</span></div>
      <div class="kv"><span>Total cost</span><span class="y" id="s-cost">$0.000000</span></div>
      <div class="kv"><span>Tool calls</span><span class="t" id="s-tools">0</span></div>
      <div class="kv"><span>Feedback 👍</span><span class="g" id="s-up">0</span></div>
      <div class="kv"><span>Feedback 👎</span><span class="r" id="s-down">0</span></div>
    </div>

    <div class="tele-sec" style="padding-bottom:16px">
      <div class="tele-lbl">Data Flow</div>
      <div class="flow">
        <div class="fnode user-node">🌐 Browser</div>
        <div class="farrow">↓ HTTP POST /api/chat</div>
        <div class="fnode app-node">⚡ Python Server<div style="font-size:9px;color:#6e7681;margin-top:2px">PII check · A/B route · OTel SDK</div></div>
        <div class="farrow">↓ function calling</div>
        <div class="fnode tool-node">🔧 Tools<div style="font-size:9px;color:#6e7681;margin-top:2px">search · calculate · stocks</div></div>
        <div class="farrow">↓ generate_content</div>
        <div class="fnode llm-node">🤖 Google Gemini<div style="font-size:9px;color:#6e7681;margin-top:2px">gemini-2.5-flash · 1.5-flash</div></div>
        <div class="farrow">↓ OTLP/HTTP</div>
        <div class="fnode ingest-node">☁️ Elastic Cloud</div>
        <div class="frow3">
          <div class="fsmall">📡<br>Traces</div>
          <div class="fsmall">📋<br>Logs</div>
          <div class="fsmall">📊<br>Metrics</div>
        </div>
        <div class="farrow">↓ Kibana</div>
        <div class="fnode kibana-node">🔍 Kibana APM</div>
      </div>
    </div>

  </div>
</main>

<script>
const $ = id => document.getElementById(id);
let sid=null, turns=0, totIn=0, totOut=0, totCost=0, totTools=0, totUp=0, totDown=0, cfg={};

const AB_CLASSES = ['ab-a','ab-b','ab-c','ab-d','ab-e','ab-f'];
function abClassFor(variant){
  const idx = (variant||'A').charCodeAt(0) - 65;
  return AB_CLASSES[((idx % AB_CLASSES.length) + AB_CLASSES.length) % AB_CLASSES.length];
}

fetch('/api/config').then(r=>r.json()).then(c=>{
  cfg = c;
  $('hdr-model').textContent = c.models.length + ' models (round-robin)';
  const KV_CLASSES = ['b','t','g','y','e','r'];
  $('model-pool-list').innerHTML = c.models.map((m,i)=>{
    const letter = String.fromCharCode(65+i);
    const cls = KV_CLASSES[i % KV_CLASSES.length];
    return `<div class="kv"><span>${letter}</span><span class="${cls}">${m}</span></div>`;
  }).join('');
  if (c.apm_url) { $('apm-link').href = c.apm_url; }
  else { $('apm-link').style.display = 'none'; }  // no KIBANA_ENDPOINT configured
  $('psutil-dot').className = 'dot-s ' + (c.psutil ? 'ok' : 'dim');
});

const ta = $('inp');
ta.addEventListener('input', ()=>{ ta.style.height='42px'; ta.style.height=Math.min(ta.scrollHeight,120)+'px'; });
ta.addEventListener('keydown', e=>{ if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();send();} });

document.querySelectorAll('.scenario-card').forEach(card=>{
  card.onclick = ()=>{
    if($('send-btn').disabled) return; // a turn is already in flight
    ta.value = card.dataset.prompt;
    ta.dispatchEvent(new Event('input'));
    send();
  };
});

$('new-btn').onclick = ()=>{
  sid=null; turns=totIn=totOut=totCost=totTools=totUp=totDown=0; statUpdate();
  $('t-variant').textContent='—'; $('t-model-used').textContent='—';
  $('hdr-ab').textContent='—'; $('hdr-ab').className='badge';
  $('msgs').innerHTML=`<div class="welcome"><h2>New Conversation</h2><p>Fresh session — new A/B variant assigned. Traces appear in APM as a new conversation group.</p></div>`;
};

function md(t){
  return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/```([\s\S]*?)```/g,'<pre><code>$1</code></pre>')
    .replace(/`([^`]+)`/g,'<code>$1</code>')
    .replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>')
    .replace(/\*(.+?)\*/g,'<em>$1</em>')
    .replace(/^#{1,3}\s(.+)$/gm,'<strong>$1</strong>')
    .replace(/^[-*]\s(.+)$/gm,'&nbsp;&nbsp;• $1')
    .replace(/\n/g,'<br>');
}

async function send(){
  const msg=ta.value.trim(); if(!msg) return;
  ta.value=''; ta.style.height='42px';
  $('send-btn').disabled=true;
  const w=$('msgs').querySelector('.welcome'); if(w) w.remove();
  addBubble('user', msg);
  const typing=addTyping();
  try{
    const body={message:msg}; if(sid) body.session_id=sid;
    const r=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    typing.remove(); sid=d.session_id;

    // Tool call chips (shown before agent bubble)
    if(d.tool_calls && d.tool_calls.length>0) addToolChips(d.tool_calls);

    addBubble('agent', d.text);
    const msgIdx=turns;
    addPills(d);
    addFeedback(d.trace_id, msgIdx);
    updateAB(d);
    update(d);
  }catch(e){typing.remove();addBubble('agent','**Error:** '+e.message);}
  finally{$('send-btn').disabled=false; ta.focus();}
}
$('send-btn').onclick=send;

function addBubble(who, text){
  const d=document.createElement('div'); d.className='msg '+who;
  d.innerHTML=`<div class="avatar">${who==='user'?'👤':'⚡'}</div><div class="bubble">${md(text)}</div>`;
  $('msgs').appendChild(d); scroll(); return d;
}

function addToolChips(tools){
  const wrap=document.createElement('div'); wrap.className='tools-row';
  tools.forEach((t,i)=>{
    const chip=document.createElement('div'); chip.className='tool-chip';
    let argsPreview;
    try{ argsPreview=JSON.stringify(JSON.parse(t.args_summary||'{}')).slice(0,40); }
    catch(e){ argsPreview=(t.args_summary||'').slice(0,40); }
    chip.innerHTML=`<span>🔧</span><span class="tc-name">${t.name}(${argsPreview})</span><span class="tc-dur">${t.duration_ms}ms</span>`;
    const detail=document.createElement('div'); detail.className='tool-detail';
    detail.textContent=`→ ${t.result_summary}`;
    chip.onclick=()=>detail.classList.toggle('open');
    wrap.appendChild(chip); wrap.appendChild(detail);
  });
  $('msgs').appendChild(wrap); scroll();
}

function addPills(d){
  const r=document.createElement('div'); r.className='pills-row';
  const abClass=abClassFor(d.variant||'A');
  r.innerHTML=`
    <a class="pill trace" href="${cfg.apm_url||'#'}" target="_blank">📡 ${(d.trace_id||'').substring(0,8)}…</a>
    <span class="pill otlp">OTLP</span>
    <span class="pill ${abClass}">${d.variant||'A'}:${(d.model_used||'').split('-').slice(-2).join('-')}</span>
    <span class="pill dim">${(d.in_tokens||0).toLocaleString()}↑ ${(d.out_tokens||0).toLocaleString()}↓</span>
    <span class="pill dim">${d.latency_ms}ms</span>
    <span class="pill dim">$${(d.cost_usd||0).toFixed(5)}</span>
    ${d.pii_detected?'<span class="pill pii">⚠️ PII</span>':''}
    ${d.is_error?'<span class="pill err">error</span>':''}`;
  $('msgs').appendChild(r); scroll();
}

function addFeedback(traceId, msgIdx){
  const r=document.createElement('div'); r.className='feedback-row';
  r.innerHTML=`<span class="fb-lbl">Helpful?</span>
    <button class="fb-btn" id="up-${msgIdx}">👍</button>
    <button class="fb-btn" id="dn-${msgIdx}">👎</button>`;
  $('msgs').appendChild(r);

  const vote=(rating)=>{
    fetch('/api/feedback',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({session_id:sid,trace_id:traceId,rating})});
    $(`up-${msgIdx}`).className='fb-btn'+(rating===1?' voted-up':'');
    $(`dn-${msgIdx}`).className='fb-btn'+(rating===-1?' voted-down':'');
    if(rating===1){totUp++;$('s-up').textContent=totUp;}
    else{totDown++;$('s-down').textContent=totDown;}
  };
  $(`up-${msgIdx}`).onclick=()=>vote(1);
  $(`dn-${msgIdx}`).onclick=()=>vote(-1);
  scroll();
}

function addTyping(){
  const d=document.createElement('div'); d.className='msg agent';
  d.innerHTML='<div class="avatar">⚡</div><div class="typing"><div class="dots"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>Thinking…</div>';
  $('msgs').appendChild(d); scroll(); return d;
}
function scroll(){ const m=$('msgs'); m.scrollTop=m.scrollHeight; }

function updateAB(d){
  $('t-variant').textContent = d.variant||'A';
  $('t-model-used').textContent = d.model_used||'—';
  const abClass=abClassFor(d.variant||'A');
  $('hdr-ab').textContent=(d.variant||'A')+': '+(d.model_used||'—');
  $('hdr-ab').className='badge '+abClass;
}

function update(d){
  $('t-lat').textContent   = d.latency_ms+'ms';
  $('t-in').textContent    = (d.in_tokens||0).toLocaleString();
  $('t-out').textContent   = (d.out_tokens||0).toLocaleString();
  $('t-cost').textContent  = '$'+(d.cost_usd||0).toFixed(5);
  $('t-tools').textContent = (d.tool_calls||[]).length;
  $('t-tid').textContent   = d.trace_id||'—';
  turns++; totIn+=d.in_tokens||0; totOut+=d.out_tokens||0;
  totCost+=d.cost_usd||0; totTools+=(d.tool_calls||[]).length;
  statUpdate();
}
function statUpdate(){
  $('s-turns').textContent = turns;
  $('s-in').textContent    = totIn.toLocaleString();
  $('s-out').textContent   = totOut.toLocaleString();
  $('s-cost').textContent  = '$'+totCost.toFixed(6);
  $('s-tools').textContent = totTools;
}
</script>
</body>
</html>"""


# ── HTTP server ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass

    def send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/config":
            return self.send_json(200, {
                "models":           MODEL_POOL,
                "host":             HOST_NAME,
                "metrics_interval": METRICS_INTERVAL_S,
                "psutil":           _PSUTIL,
                "otlp_endpoint":    OTEL_ENDPOINT,
                "apm_url":          _apm_url(f"/app/apm/services/{SVC_NAME}/overview"),
            })
        body = HTML.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/api/chat":
            sid, session = get_or_create_session(body.get("session_id"))
            try:
                result = chat_turn(
                    body["message"],
                    session["conv_id"],
                    session["history"],
                    session["model"],
                    session["variant"],
                )
                session["history"].append({"role": "user",  "text": body["message"]})
                session["history"].append({"role": "model", "text": result["text"]})
                session["history"] = session["history"][-40:]
                result["session_id"] = sid
                self.send_json(200, result)
            except Exception as e:
                logger.exception("chat_turn failed")
                self.send_json(500, {"error": str(e), "session_id": sid})

        elif self.path == "/api/feedback":
            # User thumbs up/down — log as OTel record correlated to trace_id
            rating    = body.get("rating", 0)       # 1 = positive, -1 = negative
            trace_id  = body.get("trace_id", "")
            session_id = body.get("session_id", "")

            label = "positive" if rating > 0 else "negative"
            feedback_counter.add(1, {
                "feedback.rating":      label,
                "gen_ai.provider.name": GENAI_PROVIDER,
            })
            logger.info("User feedback received", extra={
                "feedback.rating":     label,
                "feedback.score":      rating,
                "feedback.trace_id":   trace_id,
                "session_id":          session_id,
            })
            self.send_json(200, {"ok": True})

        else:
            self.send_json(404, {"error": "not found"})


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if _PSUTIL:
        psutil.cpu_percent(interval=None)
        threading.Thread(target=_psutil_scrape_loop, daemon=True, name="psutil-scrape").start()

    logger.info(f"Web agent v2 starting — model_pool={MODEL_POOL} "
                f"host={HOST_NAME} port={PORT} psutil={_PSUTIL}")

    print(f"\n{'─'*64}")
    print(f"  Elastic AI Agent — Full OTel Demo  v2.0")
    print(f"{'─'*64}")
    print(f"  Chat UI  →  http://localhost:{PORT}")
    if KB_ENDPOINT:
        print(f"  APM UI   →  {KB_ENDPOINT}/app/apm/services/{SVC_NAME}/overview")
    else:
        print(f"  APM UI   →  (set KIBANA_ENDPOINT in .env to get a clickable link)")
    print(f"  Host     :  {HOST_NAME}")
    print(f"  Models   :  round-robin across {len(MODEL_POOL)}")
    for i, m in enumerate(MODEL_POOL):
        print(f"    {chr(ord('A') + i)}: {m}")
    print(f"{'─'*64}")
    print(f"  OTel signals → {OTEL_ENDPOINT}")
    print(f"    📡 Traces  (+ tool spans)  → traces-generic.otel-default")
    print(f"    📋 Logs    (+ feedback)    → logs-generic.otel-default")
    print(f"    📊 Metrics (gen_ai.*, guardrails, feedback, host)")
    print(f"{'─'*64}")
    print(f"  Features: tool calling · A/B routing · PII guardrails · feedback loop")
    print(f"{'─'*64}\n")

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down…")
    finally:
        metric_reader.shutdown()
        trace_provider.shutdown()
        log_provider.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
