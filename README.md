# Kaisos

A personal AI agent in **one Python file with zero dependencies**. It reads, writes
and edits files, runs shell commands, searches your project and the web — looping
with tools until the task is done. It remembers things between sessions, teaches
itself skills, **runs itself** (heartbeat, file watchers, task inbox, boot-time
service), can see images, speaks **MCP** to use the whole ecosystem of external
tools, and you can drive it from your phone over **Telegram or WhatsApp** — or from any browser tab through a token-protected web cockpit.

Two interchangeable brains:

| Backend | Needs | Notes |
|---|---|---|
| **Anthropic API** | `ANTHROPIC_API_KEY` | best quality; prompt caching automatic |
| **Ollama (local)** | [ollama.com](https://ollama.com) + `ollama pull qwen3` | fully offline, free |

## Quick start

```bash
export ANTHROPIC_API_KEY=sk-ant-...     # or: ollama pull qwen3
python3 kaisos.py                        # interactive chat
python3 kaisos.py -p "task" --yes        # one-shot
python3 kaisos.py --daemon               # automation loop (see below)
python3 kaisos.py --install-service      # make the daemon permanent
#   → then open http://127.0.0.1:8484 to chat with it in the browser
```

The agent operates inside the folder you start it from (or `--workspace DIR`).
All state lives in `<workspace>/.agent/` as plain text you can read and edit.

## New in v3.2 — the cockpit release

**Web chat cockpit.** The dashboard is no longer read-only. While `--daemon`
runs, open **http://127.0.0.1:8484** and you get a full conversation with the
agent in the browser: it streams replies, and when it wants to run a risky tool
the diff/command shows up inline with **approve / always / deny** buttons —
the same confirmation gate as the terminal, just clickable. Header buttons
**pause automation**, **run the heartbeat now**, **process the inbox**, and
**undo** everything this daemon session touched. You can cancel any scheduled
job or watch from its card.

*Auth, honestly.* GETs that only read (status panels) work on loopback without a
token. **Every mutating endpoint — chat, confirm, actions — requires a token**,
written to `.agent/dash_token` (mode 600) on first run. On a loopback browser
the page injects the token for you, so locally it Just Works. Bind it wider with
`--dash-bind 0.0.0.0` (or `AGENT_DASH_BIND`) to reach it from your phone on the
LAN — then *every* request, including the page itself, demands the token, which
is printed to the console at startup. A foreign `Host:` header still gets a 403.

**Usage ledger + budget.** Every API call's tokens are tallied per-day in
`.agent/usage.json` (input / output / cache, plus background-task count), shown
live on the dashboard and via `/usage` in the REPL. Set **`AGENT_DAILY_BUDGET`**
(tokens/day) and once background automation crosses it, jobs/watches/heartbeat
skip with a logged note and a single notification — interactive use is never
blocked. **`AGENT_DAEMON_MODEL`** routes only unattended runs to a cheaper model
while you keep the strong one for live chat.

**MCP self-healing.** If a local (stdio) MCP server crashes, the next call to one
of its tools respawns it and retries once — rate-limited to one restart a minute
so a genuinely broken server can't spin. The retry is tagged in the result.

**`search_memory`.** A read-only tool that greps *all* of memory and skills,
including notes too old to fit in the auto-injected window — so the agent can
recall something from months ago instead of only what's freshly in context.

## New in v3.1 — hardening + dashboard

*(v3.2 above turns this dashboard into an interactive, token-authenticated chat
cockpit; the description below is how it debuted in v3.1 — read-only.)*

**Dashboard** — while `--daemon` runs, a read-only status page is served at
**http://127.0.0.1:8484** (`--dash-port`, `--no-dash`, `AGENT_BRAND` renames it):
live activity log, scheduled jobs, file watches, heartbeat countdown, memory,
skills, MCP servers, git state. Kaisos purple/gold, single embedded page, zero
external assets. Security posture: bound to 127.0.0.1 only, GET-only (no state
can be changed from a browser, so a malicious website can't CSRF it), and a
foreign `Host:` header gets a 403 to block DNS-rebinding probes.

**Git awareness** — in a git repo the agent's instructions include the branch and
dirty-file count, tell it to prefer checkpoint commits via `run_command`, and
hard-forbid push/merge/rebase unless you ask. (Deliberately *not* auto-branching
and auto-merging: an agent silently merging is the wrong direction for safety —
you review, you merge.)

**`--fallback-local`** — unattended daemon tasks (jobs, watches, heartbeat, inbox)
that hit an API outage or rate limit retry once on local Ollama, so automation
keeps working offline. Interactive sessions don't switch mid-conversation (the
two backends store history in incompatible formats); fallback runs are tagged
`fallback` in jobs.log and in the notification.

**Four bugs fixed** (each now has a regression test that fails on v3.0):
the parallel executor gated on "confirm-free" but `remember`/`save_skill`/
`delegate` mutate state — now an explicit read-only allowlist; a task that
scheduled a job *while the scheduler was mid-pass* had its job silently
clobbered — the job store is now lock-protected (`jobs.lock`) with atomic
temp-file-rename writes and per-mutation fresh reads; Telegram messages sent
during a pending confirmation were consumed and dropped — they're now queued
and processed after you answer; and base64 images inflated the token estimate
~13×, triggering premature compaction — images now count as ~1.5k tokens and
are stripped from compaction digests.

## v3 — the automation release

**The agent no longer just waits for you.** With `--daemon` running (or installed
as a service), four automation channels are live:

1. **Heartbeat** — put standing instructions in `.agent/HEARTBEAT.md` ("if anything
   in inbox-drafts/ is older than a day, remind me"). The agent reads them every
   30 minutes (`AGENT_HEARTBEAT_MIN`) and acts only if something needs doing.
   A file with only `#`-comments costs nothing — no API call is made.
2. **File watchers** — *"whenever demos/*.wav changes, normalize the filenames"*
   creates a watch via the `watch_path` tool. The daemon polls, waits for files to
   settle (so half-copied files don't trigger), then runs the task with the changed
   filenames appended. Max 10 watches; list/cancel like any job.
3. **Task inbox** — drop a `.txt`/`.md` file into `.agent/inbox/` from anywhere
   (a script, cron, phone file sync, another program); the daemon runs its content
   as a task and archives the file to `inbox/done/`. The unix pipe of agents.
4. **Service install** — `--install-service` registers the daemon with **systemd**
   (Linux, user unit + chmod-600 env file), **launchd** (macOS plist) or **Task
   Scheduler** (Windows). Survives reboots; `--uninstall-service` removes it.

Plus three quieter upgrades:

- **Auto-reflection** — at the end of each interactive session the agent extracts
  up to 3 durable facts into memory, tagged `[auto]` so you can spot and prune them
  (`--no-reflect` disables).
- **Parallel tools** — when the model requests several read-only tools in one step
  (multiple file reads, searches), they execute concurrently.
- **Desktop notifications** — daemon results pop up via `notify-send`/osascript
  when Telegram isn't paired.

## MCP — Model Context Protocol

The agent is a full **MCP client**: point it at any MCP server and its tools join
the agent's toolbox. Create `.agent/mcp.json` (same format as Claude Desktop):

```json
{
  "mcpServers": {
    "files":  { "command": "npx", "args": ["-y", "@modelcontextprotocol/server-filesystem", "/home/adnan/music"] },
    "github": { "command": "npx", "args": ["-y", "@modelcontextprotocol/server-github"],
                "env": { "GITHUB_TOKEN": "ghp_..." } },
    "remote": { "url": "https://example.com/mcp" }
  }
}
```

- **stdio servers** (`command` + `args` + optional `env`) are spawned as
  subprocesses — JSON-RPC over pipes; their stderr lands in `.agent/mcp-<name>.log`.
- **HTTP servers** (`url`) use streamable HTTP: JSON or SSE responses and
  `Mcp-Session-Id` round-tripping are both handled.
- Discovered tools register as **`mcp__<server>__<tool>`** and — being third-party
  code — **always ask for confirmation** before running (subagents never get them).
- MCP image results flow straight into the vision pipeline.
- `/mcp` in the REPL shows connected servers and their tools; `--no-mcp` skips
  connecting entirely.

⚠ MCP servers are arbitrary programs running with **your** permissions. Only
configure servers you trust, exactly as you would any installed software.

## Everyday use

REPL commands: `/help /tools /memory /skills /jobs /mcp /undo /compact /reset /quit`

Risky tools (write_file, edit_file, run_command, schedule/watch/cancel, all MCP
tools) ask first — writes show a colored **diff** — answer `y` / `n` / `a` (always
this session). `--yes` skips prompts. `/undo` rolls back every file the session
touched (snapshots in `.agent/backups/`, last 10 sessions kept).

## Memory & skills

`remember` appends dated notes to `.agent/memory.md` (always injected); reflection
adds `[auto]` notes on its own. `save_skill` stores how-tos in `.agent/skills/`;
their index is always visible and `read_skill` loads one on demand. Over time the
agent stops rediscovering your deploy steps and conventions.

## Scheduling

`"in 30 minutes"` · `"every 2 hours"` · `"every 3 days"` · `"every day at 09:00"` ·
`"every weekday at 09:00"` · `"every monday at 18:00"` · `"tomorrow at 08:00"` ·
`"once at 2026-07-01 18:00"` — natural-language jobs persist in `.agent/jobs.json`,
run with fresh context in the daemon, log to `.agent/jobs.log`, and notify via
Telegram or desktop.

Every run records its outcome on the job: `list_scheduled` (and the dashboard)
show each job's run/fail counts and last-run time. A **recurring job that fails
`AGENT_MAX_JOB_FAILS` times in a row (default 5) auto-disables itself** and sends
one notification — no silent loop burning tokens. Fire any job on demand with the
`run_job` tool, the dashboard's **▸** button, or `--run-job <id>` (a manual success
re-enables a disabled job). Poll cadence is `AGENT_TICK_SEC` (default 20s).

## Telegram

```bash
TELEGRAM_BOT_TOKEN=123:abc python3 kaisos.py --daemon    # or --telegram (no automation)
```

First account to message the bot becomes its owner (`.agent/config.json`); others
are ignored. Confirmations arrive in chat (y/n/a, 3-min timeout declines). Send a
photo → saved to the workspace, ready for `read_image`. Commands:
`/jobs /undo /reset /quit`.

## WhatsApp

Same phone-driven control as Telegram, over Meta's **official WhatsApp Cloud
API** (no unofficial WhatsApp-Web automation — that violates WhatsApp's terms and
gets numbers banned). Setup is more involved than Telegram because Meta requires
a public HTTPS webhook to deliver inbound messages.

**One-time setup**

1. Create a free Meta developer app at *developers.facebook.com* → add the
   **WhatsApp** product. It gives you a test **phone-number ID** and a temporary
   access token (swap in a permanent token for real use).
2. In the app's **App settings → Basic**, copy the **App Secret** — inbound
   messages are HMAC-SHA256 signed with it.
3. Choose any string as your **verify token** (it's just a shared secret for the
   one-time webhook handshake).

```bash
export WHATSAPP_TOKEN=EAAG...            # Cloud API access token
export WHATSAPP_PHONE_ID=123456789       # "phone number ID" from the dashboard
export WHATSAPP_VERIFY_TOKEN=any-secret  # you choose this string
export WHATSAPP_APP_SECRET=ab12...       # REQUIRED — no secret, no inbound
python3 kaisos.py --daemon
```

The webhook **rides the dashboard server** at `/webhook/whatsapp` — you don't run
a second port. Expose **only that path** over HTTPS with a tunnel or reverse
proxy, e.g. `cloudflared tunnel --url http://localhost:8484`, then in the Meta
dashboard set the callback URL to `https://<your-tunnel>/webhook/whatsapp`, enter
the same verify token, and subscribe to the **messages** field. The agent answers
Meta's verification challenge automatically.

**Security.** Enabling WhatsApp auto-hardens the cockpit (assumes the port is now
tunnel-reachable): the web UI stops auto-injecting its token and demands it on
every request, loopback included. Inbound webhook requests are refused unless
`WHATSAPP_APP_SECRET` is set **and** the signature verifies — an unsigned or
mis-signed POST gets a 403 and never reaches the agent.

**Use.** The first number to message the bot becomes its owner (saved in
`config.json`); other numbers are ignored. After that it's the full agent —
files, shell, web, memory, MCP — with confirmations delivered as
`⚠ allow <tool>?  …  Reply y / n / a`. Send a photo and it's saved to the
workspace for `read_image`. Commands: `/reset` `/jobs` `/undo` `/usage` `/quit`.

> **24-hour window.** Meta only lets a business send free-form messages within 24h
> of the user's last message. Outside that window (e.g. a heartbeat alert at 3am
> after a quiet day) delivery may require a pre-approved template; just message the
> bot again to reopen the window. This is a WhatsApp platform rule, not an agent
> limitation.

## Subagents

`delegate` runs up to 5 parallel **read-only** researchers (no writes, no shell,
no MCP) with fresh contexts; only their short reports return.

## Safety model

Workspace sandbox (`--anywhere` lifts) · diff-gated writes · session `/undo` ·
read-only subagents · MCP tools always confirm · untrusted-web marking with an
explicit no-instruction-following rule · loop detection (3 identical calls → stop)
· heartbeat/watcher tasks only ever run instructions **you** wrote.

## Flags

```
-p/--prompt TEXT   one-shot task          --no-stream        disable streaming
--backend X        auto|anthropic|ollama  --no-reflect       no auto-memory
--model NAME       override model         --no-mcp           skip MCP servers
--workspace DIR    operate in DIR         --resume           continue last session
--anywhere         lift file sandbox      --daemon           automation loop
--yes              skip confirmations     --install-service / --uninstall-service
--fallback-local   daemon tasks retry on local Ollama when the API fails
--dash-port N      dashboard port (default 8484)        --no-dash   disable it
--dash-bind ADDR   dashboard bind address (default 127.0.0.1; wider = token on
                   every request, printed at startup)
```

Env: `ANTHROPIC_API_KEY` `AGENT_MODEL` `AGENT_OLLAMA_MODEL` `OLLAMA_HOST`
`TELEGRAM_BOT_TOKEN` `AGENT_COMPACT_TOKENS` `AGENT_HEARTBEAT_MIN` `AGENT_NUM_CTX`
`ANTHROPIC_BASE_URL` `TELEGRAM_API_BASE` `AGENT_DASH_PORT` `AGENT_DASH_BIND`
`AGENT_FALLBACK_LOCAL` `AGENT_BRAND` `AGENT_DAILY_BUDGET` `AGENT_DAEMON_MODEL`
`AGENT_TICK_SEC` `AGENT_MAX_JOB_FAILS`
`WHATSAPP_TOKEN` `WHATSAPP_PHONE_ID` `WHATSAPP_VERIFY_TOKEN` `WHATSAPP_APP_SECRET`
`WHATSAPP_API_BASE`

## Tools

17 built-in: read_file · read_image · write_file · edit_file · list_dir ·
search_files · run_command · fetch_url · web_search · remember · save_skill ·
read_skill · search_memory · delegate · schedule_task · watch_path · run_job ·
list_scheduled · cancel_scheduled — **plus anything your MCP servers expose.**

## Honest limitations

- Heartbeats and watchers spend tokens when their instructions are active — that's
  the point, but keep `HEARTBEAT.md` lean and intervals sane.
- Automation runs auto-approved (there's nobody to ask). It only executes
  instructions you wrote; still, write them like you mean them. Cap unattended
  spend with `AGENT_DAILY_BUDGET`.
- WhatsApp's 24-hour messaging window can delay off-hours notifications (see that
  section); it's a platform rule, not something the agent can bypass.
- `--install-service` shells out to systemctl/launchctl/schtasks — the generated
  files are tested, the activation depends on your system being normal.
- Web search scrapes DuckDuckGo and can break; small local models lose long plots;
  Ollama vision needs a vision model; injection guard is mitigation, not guarantee.
- MCP trust is on you (see warning above).

## Files

```
.agent/
  memory.md      notes (manual + [auto])    HEARTBEAT.md   standing instructions
  skills/        saved how-tos              inbox/         drop task files here
  jobs.json      jobs + watches             jobs.log       run history
  mcp.json       MCP server config          mcp-*.log      MCP server stderr
  config.json    telegram/whatsapp pairing   session.json   --resume state
  usage.json     per-day token ledger        dash_token     cockpit access token
  backups/       /undo snapshots            heartbeat_last timestamp
  jobs.lock      transient cross-process lock for the job store
```

## License

Apache License 2.0 — free to use, modify, and build on, including commercially.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).

© 2026 Adnan Cengiz. Independent project; not affiliated with or endorsed by
Anthropic, Meta/WhatsApp, or Telegram. It works with the Anthropic API and other
services using credentials **you** supply.

## Support this project

If it's useful to you, you can support development:

- **Ko-fi:** https://ko-fi.com/kaisos

Sponsoring is appreciated but never required — the project is and stays
Apache-2.0.
