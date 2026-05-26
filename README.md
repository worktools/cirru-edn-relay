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
edn-relay send --channel demo '{} (:op |ping) (:value 1)'
edn-relay poll --channel demo
```

## `genui` Channel Quick Start

Start the relay:

```bash
edn-relay serve
```

Then open `edn-renderer` in a browser so it subscribes to the `genui` channel.

You can let the browser create or preselect a channel with a URL such as `http://127.0.0.1:3012/?channel=genui`, or open the plain page and choose from the active channels list. If the relay port was changed earlier and stored in `~/.config/edn-relay.cirru`, the browser can also use `?port=<PORT>` to follow that port.

Send a receiver-defined payload from the CLI through the `genui` channel:

```bash
edn-relay send --channel genui '<CIRRU_EDN_PAYLOAD>'
```

The exact payload schema is defined by the receiver. For `edn-renderer`, check the receiver-side docs and runtime protocol instead of the relay README.

Expected result:

- CLI receives an ack payload containing `:status |ok` and `:layout_id`
- the browser stores the payload data and renders the layout
- the browser replies with an ack payload containing `:status |ok` and `:layout_id`
- when multiple receivers are attached to the same channel, the sender only accepts the first ack; later replies become warnings on the extra receivers

## Development

```bash
cargo check
cargo fmt
```

Protocol details live in `PROTOCOL.md`.
Development constraints live in `Agents.md`.
