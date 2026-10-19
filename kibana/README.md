# Chatbot Dependencies dashboard panel

A custom visualization panel for a Kibana dashboard that shows, per time range:
which LLM model actually served each turn (with token usage and success rate),
which tools were called via the separate MCP server (grouped by MCP server
name), and which tools ran in-process in the agent itself — all derived from
the OTel traces `server.py`/`mcp_server.py` already ship to Elastic.

![Chatbot dependencies panel](screenshots/dashboard-panel.jpg)

> The screenshot above is a local HTML re-render of this template with sample
> data (produced for this README), not a live Kibana screenshot — useful to
> see the layout before you build it, but the real panel in your own project
> will show your own trace data.

## Requirements

**This only works on an Elastic Cloud Serverless Observability project.** The
"custom visualization" dashboard panel type (ES|QL data source + a Liquid/HTML
render template) that this artifact relies on is a Serverless-only Kibana
feature — it is not available on classic (non-serverless) Elastic Cloud
deployments or self-managed clusters as of this writing. If you're running the
rest of this demo (`web_agent/`) against a non-serverless deployment, the chat
agent and MCP server will still work and ship full OTel traces/logs/metrics —
you just won't be able to add this specific panel type to a dashboard; you can
still explore the same data via Discover/APM using the ES|QL query in
`chatbot-dependencies.esql` directly.

## Setup

1. Run the demo (`web_agent/server.py` + `web_agent/mcp_server.py`, see the
   top-level [README](../README.md)) and send a handful of chat messages —
   this generates the trace data the panel visualizes. Try a few of the
   built-in **Demo Scenarios** in the chat UI's sidebar to get a mix of local
   tools, MCP tools, and the investor-briefing skill.
2. In Kibana, open **Dashboards** → **Create dashboard**.
3. **Add panel** → choose the custom visualization / ES|QL panel type (exact
   wording may vary by Kibana version — look for a panel type that accepts a
   raw ES|QL query plus an HTML render template).
4. Paste the contents of [`chatbot-dependencies.esql`](chatbot-dependencies.esql)
   into the query editor.
5. Paste the contents of [`chatbot-dependencies-panel.html`](chatbot-dependencies-panel.html)
   into the template/render editor.
6. Save the panel, save the dashboard, and set the dashboard's time range to
   cover when you ran the demo.

## Files

| File | Purpose |
|------|---------|
| `chatbot-dependencies.esql` | The ES|QL query — classifies each span into LLM/MCP-tool/direct-tool/skill/other, aggregates call count, success rate, and (for LLM rows) token usage. |
| `chatbot-dependencies-panel.html` | The Liquid + HTML/CSS render template — turns the query's rows into the collapsible panel UI. Pure CSS (checkbox/label accordion), no JavaScript — this panel type strips `<script>` and `<details>` tags. |

## Notes on the implementation

- **MCP server identity** (`mcp_server` column) comes from the `service.name`
  span attribute, which `server.py` populates by actually calling the MCP
  server's `initialize` method and reading `serverInfo.name` back — not a
  hardcoded label. See `_init_mcp_server()` in `web_agent/server.py`.
- **LLM model** (`llm_model` column) comes from the `gen_ai.request.model`
  span attribute (OTel GenAI semantic conventions), not from `peer.service`
  (which only ever says the generic `google_gemini` provider name).
- **MCP vs. direct tool classification** uses `attributes.mcp.method.name IS NOT NULL`
  as the signal — this is set only on tool spans actually dispatched over MCP
  JSON-RPC, so it reflects the real dispatch path rather than guessing from a
  naming convention.
- Collapsible sections default: LLM Provider / Tools (via MCP) / Tools (direct)
  open, Skills / Other collapsed. Each MCP server's own tool list is collapsed
  by default — expand a server to see its individual tools.
