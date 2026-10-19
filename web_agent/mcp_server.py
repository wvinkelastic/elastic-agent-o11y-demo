#!/usr/bin/env python3
"""
web_agent/mcp_server.py — Standalone MCP server (Streamable HTTP) → Elastic via OTLP

Serves three tools over real Model Context Protocol JSON-RPC (get_stock_price →
get_company_news / get_analyst_rating — the same dependency chain server.py used to
run in-process). This is a genuinely separate service: its own OTel resource/service.name,
its own process, reached over the network by server.py's MCP client.

Instrumented per the OTel MCP semantic conventions
(https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/mcp.md):
  - SERVER-kind span per tools/call, named "tools/call {tool_name}"
  - Trace context extracted from the MCP request's params._meta (NOT the HTTP layer —
    MCP context propagation is transport-independent, per the spec)
  - mcp.server.operation.duration histogram

Exports traces/metrics/logs to the SAME Elastic backend as server.py (same
OTEL_EXPORTER_OTLP_ENDPOINT / ES_API_KEY), landing in the same
traces-generic.otel-default / metrics-generic.otel-default indices — Kibana's APM
Service Map will show this as a second service connected to gemini-demo-agent via
the propagated trace context.
"""

import os, sys, time, json, socket, logging, random
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

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

# ── Config ────────────────────────────────────────────────────────────────────

OTEL_ENDPOINT = os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"]
ES_API_KEY    = os.environ["ES_API_KEY"]
KB_ENDPOINT   = os.environ.get("KIBANA_ENDPOINT", "")  # optional — only used for "open in Kibana" links

SVC_NAME  = os.environ.get("MCP_SVC_NAME", "mcp-tools-server")
HOST_NAME = os.environ.get("HOST_NAME", socket.gethostname())
MCP_PORT  = int(os.environ.get("MCP_PORT", "5603"))

MCP_PROTOCOL_VERSION = "2025-06-18"

# OTLP endpoints — Elastic's own managed OTLP endpoint (OTEL_EXPORTER_OTLP_ENDPOINT),
# not guessed by string-substituting the Elasticsearch endpoint. Full paths
# required (the base URL alone causes a silent 404).
OTLP_TRACES_EP  = f"{OTEL_ENDPOINT}/v1/traces"
OTLP_METRICS_EP = f"{OTEL_ENDPOINT}/v1/metrics"
OTLP_LOGS_EP    = f"{OTEL_ENDPOINT}/v1/logs"
OTLP_HEADERS    = {"Authorization": f"ApiKey {ES_API_KEY}"}

# ── OTel Resource — a distinct service from gemini-demo-agent, same Elastic backend ────

resource = Resource.create({
    "service.name":            SVC_NAME,
    "service.version":         "1.0.0",
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
tracer = trace.get_tracer(SVC_NAME, "1.0.0")

# ── Metrics ───────────────────────────────────────────────────────────────────

metric_exporter = OTLPMetricExporter(endpoint=OTLP_METRICS_EP, headers=OTLP_HEADERS)
metric_reader   = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=30_000)
meter_provider  = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(SVC_NAME, "1.0.0")

# mcp.server.operation.duration — https://github.com/open-telemetry/semantic-conventions-genai
mcp_op_duration = meter.create_histogram(
    "mcp.server.operation.duration", unit="s",
    description="MCP request duration as observed on the receiver")

# ── Logs ──────────────────────────────────────────────────────────────────────

log_exporter = OTLPLogExporter(endpoint=OTLP_LOGS_EP, headers=OTLP_HEADERS)
log_provider = LoggerProvider(resource=resource)
log_provider.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))

otel_handler = LoggingHandler(level=logging.DEBUG, logger_provider=log_provider)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(SVC_NAME)
logger.addHandler(otel_handler)
logger.propagate = False

# ── Tool logic (moved from server.py's _execute_tool — same behavior) ──────────

_COMPANY_IDS = {}  # company_id -> ticker, populated as get_stock_price is called

TOOL_SCHEMAS = {
    "get_stock_price": {
        "description": (
            "Look up the current or recent price and basic info for a stock ticker symbol. "
            "Returns a company_id needed by get_company_news and get_analyst_rating — "
            "call this first if you need either of those."
        ),
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker symbol, e.g. ESTC, GOOG, AAPL"}
            },
            "required": ["ticker"],
        },
    },
    "get_company_news": {
        "description": (
            "Get recent news headlines for a company. Requires the company_id returned "
            "by get_stock_price — you cannot call this with just a ticker symbol."
        ),
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "company_id": {"type": "string", "description": "The company_id returned by a prior get_stock_price call, e.g. CID-AAPL-4471"}
            },
            "required": ["company_id"],
        },
    },
    "get_analyst_rating": {
        "description": (
            "Get the current Wall Street analyst consensus rating (buy/hold/sell) and price "
            "target for a company. Requires the company_id returned by get_stock_price — "
            "you cannot call this with just a ticker symbol."
        ),
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "company_id": {"type": "string", "description": "The company_id returned by a prior get_stock_price call, e.g. CID-AAPL-4471"}
            },
            "required": ["company_id"],
        },
    },
}


def _run_tool(name: str, args: dict) -> str:
    """Execute a tool call and return its result string. Raises ValueError for bad input."""
    if name == "get_stock_price":
        ticker = args.get("ticker", "").upper()
        time.sleep(0.06)  # simulate API latency
        prices = {
            "ESTC": 78.42, "GOOG": 182.15, "AAPL": 227.83,
            "MSFT": 415.20, "AMZN": 196.47, "NVDA": 124.38,
        }
        price = prices.get(ticker, round(random.uniform(50, 500), 2))
        change = round(random.uniform(-3.5, 4.2), 2)
        pct    = round(change / price * 100, 2)
        sign   = "+" if change >= 0 else ""
        company_id = f"CID-{ticker}-{random.randint(1000, 9999)}"
        _COMPANY_IDS[company_id] = ticker
        return (f"{ticker}: ${price:.2f}  {sign}{change} ({sign}{pct}%)  "
                f"[simulated · {datetime.now(timezone.utc).strftime('%H:%M UTC')}]  "
                f"company_id={company_id}")

    elif name == "get_company_news":
        company_id = args.get("company_id", "")
        time.sleep(0.09)  # simulate network round-trip
        ticker = _COMPANY_IDS.get(company_id)
        if ticker is None:
            return (f"Error: unknown company_id '{company_id}'. "
                     "Call get_stock_price first to obtain a valid company_id.")
        headlines = [
            f"{ticker} announces Q3 earnings beat, raises full-year guidance",
            f"Analysts weigh in on {ticker}'s latest product roadmap",
            f"{ticker} expands cloud partnerships amid sector-wide growth",
        ]
        return f"Recent news for {ticker} ({company_id}):\n" + "\n".join(f"- {h}" for h in headlines)

    elif name == "get_analyst_rating":
        company_id = args.get("company_id", "")
        time.sleep(0.07)  # simulate API latency
        ticker = _COMPANY_IDS.get(company_id)
        if ticker is None:
            return (f"Error: unknown company_id '{company_id}'. "
                     "Call get_stock_price first to obtain a valid company_id.")
        rating = random.choice(["Strong Buy", "Buy", "Hold", "Sell"])
        target = round(random.uniform(80, 550), 2)
        return (f"Analyst consensus for {ticker} ({company_id}): {rating}, "
                f"12-month price target ${target:.2f}")

    raise ValueError(f"Unknown tool: {name}")


# ── MCP JSON-RPC handling ───────────────────────────────────────────────────────

# JSON-RPC error codes — https://www.jsonrpc.org/specification#error_object
JSONRPC_METHOD_NOT_FOUND = -32601
JSONRPC_INVALID_PARAMS   = -32602
JSONRPC_INTERNAL_ERROR   = -32603


def _handle_initialize(req_id):
    return {
        "jsonrpc": "2.0", "id": req_id,
        "result": {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SVC_NAME, "version": "1.0.0"},
        },
    }


def _handle_tools_list(req_id):
    tools = [
        {"name": name, "description": schema["description"], "inputSchema": schema["inputSchema"]}
        for name, schema in TOOL_SCHEMAS.items()
    ]
    return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools}}


def _handle_tools_call(req_id, params, client_address):
    tool_name = params.get("name", "")
    arguments = params.get("arguments", {}) or {}
    meta      = params.get("_meta", {}) or {}

    # MCP context propagation is transport-independent: the client injects
    # traceparent/tracestate into params._meta (not the HTTP layer), so we
    # extract the remote parent context from there — per the MCP semconv's
    # "Context propagation" section.
    parent_ctx = propagate.extract(meta)

    t0 = time.time()
    with tracer.start_as_current_span(
        f"tools/call {tool_name}",
        context=parent_ctx,
        kind=SpanKind.SERVER,
        attributes={
            "mcp.method.name":            "tools/call",
            "gen_ai.operation.name":      "execute_tool",
            "gen_ai.tool.name":           tool_name,
            "jsonrpc.request.id":         str(req_id) if req_id is not None else "",
            "mcp.protocol.version":       MCP_PROTOCOL_VERSION,
            "network.protocol.name":      "http",
            "network.transport":          "tcp",
            "client.address":             client_address[0],
            "client.port":                client_address[1],
            "gen_ai.tool.call.arguments": json.dumps(arguments)[:400],
        },
    ) as span:
        logger.info("Tool call received", extra={
            "mcp.method.name": "tools/call", "gen_ai.tool.name": tool_name,
            "jsonrpc.request.id": str(req_id),
        })

        error_type = None
        rpc_status = None
        try:
            if tool_name not in TOOL_SCHEMAS:
                rpc_status = str(JSONRPC_INVALID_PARAMS)
                error_type = rpc_status
                span.set_attribute("rpc.response.status_code", rpc_status)
                span.set_attribute("error.type", error_type)
                span.set_status(StatusCode.ERROR, f"Unknown tool: {tool_name}")
                response = {"jsonrpc": "2.0", "id": req_id,
                            "error": {"code": JSONRPC_INVALID_PARAMS, "message": f"Unknown tool: {tool_name}"}}
            else:
                result = _run_tool(tool_name, arguments)
                span.set_attribute("gen_ai.tool.call.result", result[:400])
                span.set_status(StatusCode.OK)
                response = {"jsonrpc": "2.0", "id": req_id,
                            "result": {"content": [{"type": "text", "text": result}]}}
        except Exception as e:
            error_type = type(e).__name__
            rpc_status = str(JSONRPC_INTERNAL_ERROR)
            span.set_attribute("rpc.response.status_code", rpc_status)
            span.set_attribute("error.type", error_type)
            span.set_status(StatusCode.ERROR, str(e))
            span.record_exception(e)
            logger.error(f"Tool execution failed: {e}", exc_info=True,
                         extra={"gen_ai.tool.name": tool_name})
            response = {"jsonrpc": "2.0", "id": req_id,
                        "error": {"code": JSONRPC_INTERNAL_ERROR, "message": str(e)}}

        elapsed = time.time() - t0
        metric_attrs = {"mcp.method.name": "tools/call", "gen_ai.operation.name": "execute_tool",
                         "gen_ai.tool.name": tool_name, "network.transport": "tcp"}
        if error_type:
            metric_attrs["error.type"] = error_type
        if rpc_status:
            metric_attrs["rpc.response.status_code"] = rpc_status
        mcp_op_duration.record(elapsed, metric_attrs)

        logger.info("Tool call completed", extra={
            "gen_ai.tool.name": tool_name, "latency_ms": round(elapsed * 1000),
            "is_error": error_type is not None,
        })

    return response


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # quiet stdlib access log — we log via `logger` instead

    def send_json(self, code: int, payload: dict):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {"ok": True, "service": SVC_NAME})
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/mcp":
            self.send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        try:
            req = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self.send_json(400, {"jsonrpc": "2.0", "id": None,
                                  "error": {"code": -32700, "message": "Parse error"}})
            return

        method = req.get("method")
        req_id = req.get("id")
        params = req.get("params", {}) or {}

        if method == "initialize":
            self.send_json(200, _handle_initialize(req_id))
        elif method == "tools/list":
            self.send_json(200, _handle_tools_list(req_id))
        elif method == "tools/call":
            self.send_json(200, _handle_tools_call(req_id, params, self.client_address))
        elif method in ("notifications/initialized",):
            self.send_response(202)
            self.end_headers()
        else:
            self.send_json(200, {"jsonrpc": "2.0", "id": req_id,
                                  "error": {"code": JSONRPC_METHOD_NOT_FOUND, "message": f"Unknown method: {method}"}})


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info(f"MCP tools server starting — host={HOST_NAME} port={MCP_PORT}")

    print(f"\n{'─'*64}")
    print(f"  MCP Tools Server — get_stock_price / get_company_news / get_analyst_rating")
    print(f"{'─'*64}")
    print(f"  MCP endpoint →  http://localhost:{MCP_PORT}/mcp")
    if KB_ENDPOINT:
        print(f"  APM UI       →  {KB_ENDPOINT}/app/apm/services/{SVC_NAME}/overview")
    else:
        print(f"  APM UI       →  (set KIBANA_ENDPOINT in .env to get a clickable link)")
    print(f"  Host         :  {HOST_NAME}")
    print(f"{'─'*64}")
    print(f"  OTel signals → {OTEL_ENDPOINT}")
    print(f"    📡 Traces  (SERVER spans, remote-parented via params._meta)")
    print(f"    📋 Logs    → logs-generic.otel-default")
    print(f"    📊 Metrics → mcp.server.operation.duration")
    print(f"{'─'*64}\n")

    server = HTTPServer(("0.0.0.0", MCP_PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down…")
    finally:
        trace_provider.shutdown()
        meter_provider.shutdown()
        log_provider.shutdown()


if __name__ == "__main__":
    main()
