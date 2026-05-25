# edn-relay

`edn-relay` is a Cirru EDN based websocket relay for CLI-to-browser workflows.

It is designed for a simple loop:

1. start a relay server from the CLI
2. keep a browser page connected as a long-lived subscriber
3. send a Cirru EDN DSL payload from the CLI
4. let the browser render it and reply with an ack

The current default frontend target is `edn-renderer`.

## Features

- Websocket relay server for browsers, CLIs, and workers
- Cirru EDN protocol envelopes for all websocket text frames
- `genui` command for sending validated layout DSL to a browser renderer
- `send` / `poll` / `reply` commands for generic message flows
- Local layout validation before `genui` sends anything over the network

## Install

Install the binary from the repository root:

```bash
cargo install --path . --force
```

This installs the command as `edn-relay`.

## Commands

```bash
edn-relay serve
edn-relay genui --server ws://127.0.0.1:9001 '<LAYOUT>'
edn-relay send --server ws://127.0.0.1:9001 --channel demo --payload '{} (:op |ping) (:value 1)'
edn-relay poll --server ws://127.0.0.1:9001 --channel demo
edn-relay reply --server ws://127.0.0.1:9001 --id <REQUEST_ID> --payload '{} (:result |pong)'
```

## `genui` Quick Start

Start the relay:

```bash
edn-relay serve
```

Then open `edn-renderer` in a browser so it subscribes to the `genui` channel.

Send a layout DSL from the CLI:

```bash
LAYOUT=$(cat <<'EOF'
{}
	:type |card
	:text "|CLI Demo"
	:children $ []
		{} (:type |badge) (:text |preview)
		{} (:type |divider)
		{} (:type |text) (:text "|Hello from installed CLI")
		{} (:type |row)
			:children $ []
				{} (:type |button) (:text |Confirm)
				{} (:type |input) (:name |email) (:placeholder |Email)
EOF
)

edn-relay genui --server ws://127.0.0.1:9001 "$LAYOUT"
```

Expected result:

- CLI prints `genui ok <layout-id>`
- the browser stores the payload data and renders the layout
- the browser replies with an ack payload containing `:status |ok` and `:layout_id`

## Layout Validation

`genui` currently validates these node types before sending:

- `column`
- `row`
- `card`
- `text`
- `badge`
- `divider`
- `button`
- `input`

Rules:

- `text`, `badge`, and `button` require a non-empty `:text`
- `input` requires `:name` or `:placeholder`
- container nodes (`column`, `row`, `card`) recursively validate `:children`

## Development

```bash
cargo check
cargo fmt
```

Protocol details live in `PROTOCOL.md`.
Development constraints live in `Agents.md`.
