# edn-relay

`edn-relay` is a Cirru EDN based websocket relay for CLI-to-browser workflows.

It is designed for a simple loop:

1. start a relay server from the CLI
2. keep a browser page connected as a long-lived receiver on one channel
3. choose a channel name for the current request
4. send a Cirru EDN DSL payload from the CLI
5. let the browser render it and return an ack

The current default frontend target is `edn-renderer`.

## Features

- Websocket relay server for browsers, CLIs, and workers
- Cirru EDN protocol envelopes for all websocket text frames
- `send` / `poll` commands for generic message flows
- Convention-based channels such as `genui` for renderer layout delivery

## Install

Install the binary from the repository root:

```bash
cargo install --path . --force
```

This installs the command as `edn-relay`.

## Commands

```bash
edn-relay serve
edn-relay channels
edn-relay help --channel genui
edn-relay skill --channel genui
edn-relay status --channel genui
edn-relay open --channel genui
edn-relay open-published --channel genui
edn-relay send --channel demo '{} (:op |ping) (:value 1)'
edn-relay poll --channel demo
```

The CLI is stateless with respect to relay context:

- command requests default to `ws://127.0.0.1:9100`
- use `--server <WS_URL>` when the relay is not on the default address
- use `--channel <NAME>` to choose the target receiver channel for each request

## `genui` Channel Quick Start

Start the relay:

```bash
edn-relay serve
```

Then open `edn-renderer` in a browser so it subscribes to the `genui` channel.

You can let the browser create or preselect a channel with a URL such as `http://127.0.0.1:3010/?channel=genui`, or open the plain page and choose from the active channels list.

If you do not already have a renderer page connected, you can bootstrap one directly from the CLI:

```bash
edn-relay open-published --channel genui
edn-relay open-published --server ws://127.0.0.1:9200 --channel genui
```

This opens the published renderer at `https://r.tiye.me/Erigeron/edn-renderer/` and passes both `?channel=` and `?server=` so the page can connect to the relay target chosen for that command.

Send a receiver-defined payload from the CLI through the `genui` channel:

```bash
edn-relay send --channel genui '<CIRRU_EDN_PAYLOAD>'
edn-relay send --server ws://127.0.0.1:9200 --channel genui '<CIRRU_EDN_PAYLOAD>'
```

The exact payload schema is defined by the receiver. For `edn-renderer`, check the receiver-side docs and runtime protocol instead of the relay README.

Expected result:

- CLI receives an ack payload containing `:status |ok` and `:layout_id`
- the browser stores the payload data and renders the layout
- the browser replies with an ack payload containing `:status |ok` and `:layout_id`
- when multiple receivers are attached to the same channel, the sender only accepts the first ack; later replies become warnings on the extra receivers

If the target channel has no active receiver, request commands such as `send`, `help`, `skill`, `status`, and `open` now fail fast with a hint that tells you to inspect active channels or start a renderer first.

## Development

```bash
cargo check
cargo fmt
```

Protocol details live in `PROTOCOL.md`.
Development constraints live in `Agents.md`.
