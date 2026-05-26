use anyhow::{Result, anyhow, bail};
use cirru_edn::{Edn, EdnListView, EdnMapView};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::process::Command as ProcessCommand;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, accept_async, connect_async};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(version, about = "Cirru EDN websocket relay", disable_help_subcommand = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Serve {
        #[arg(long)]
        bind: Option<String>,
    },
    Genui {
        layout: String,
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },
    Help {
        topics: Vec<String>,
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },
    Skill {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },
    Status {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },
    Current,
    Open {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },
    Send {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        channel: String,
        #[arg(long)]
        payload: String,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_secs: u64,
    },
    Poll {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        channel: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        client_id: Option<String>,
    },
    Reply {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        id: String,
        #[arg(long)]
        payload: Option<String>,
        #[arg(long)]
        error: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WireMessage {
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    channels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expects_reply: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ok: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<Edn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    events: Vec<QueuedEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedEvent {
    id: String,
    channel: String,
    from: String,
    payload: Edn,
}

#[derive(Debug, Deserialize)]
struct GenUiLayoutNode {
    #[serde(rename = "type")]
    node_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    placeholder: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    series: Vec<GenUiChartItem>,
    #[serde(default)]
    children: Vec<GenUiLayoutNode>,
}

#[derive(Debug, Deserialize)]
struct GenUiChartItem {
    label: String,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct GenUiAckPayload {
    layout_id: String,
    status: String,
}

impl WireMessage {
    fn hello(role: impl Into<String>, client_id: Option<String>, channels: Vec<String>) -> Self {
        Self {
            kind: "hello".into(),
            role: Some(role.into()),
            client_id,
            channels,
            ..Self::default()
        }
    }

    fn hello_ok(client_id: String) -> Self {
        Self {
            kind: "hello-ok".into(),
            client_id: Some(client_id),
            ..Self::default()
        }
    }

    fn request(id: String, channel: String, payload: Edn) -> Self {
        Self {
            kind: "request".into(),
            id: Some(id),
            channel: Some(channel),
            payload: Some(payload),
            expects_reply: Some(true),
            ..Self::default()
        }
    }

    fn accepted(id: String, channel: String, status: impl Into<String>) -> Self {
        Self {
            kind: "accepted".into(),
            id: Some(id),
            channel: Some(channel),
            status: Some(status.into()),
            ..Self::default()
        }
    }

    fn event(event: QueuedEvent) -> Self {
        Self {
            kind: "event".into(),
            id: Some(event.id),
            channel: Some(event.channel),
            payload: Some(event.payload),
            from: Some(event.from),
            ..Self::default()
        }
    }

    fn ack(id: String, ok: bool, payload: Option<Edn>, error: Option<String>) -> Self {
        Self {
            kind: "ack".into(),
            id: Some(id),
            ok: Some(ok),
            payload,
            error,
            ..Self::default()
        }
    }

    fn reply_accepted(id: String) -> Self {
        Self {
            kind: "reply-accepted".into(),
            id: Some(id),
            ..Self::default()
        }
    }

    fn poll(channel: String, limit: usize) -> Self {
        Self {
            kind: "poll".into(),
            channel: Some(channel),
            limit: Some(limit),
            ..Self::default()
        }
    }

    fn poll_result(channel: String, events: Vec<QueuedEvent>) -> Self {
        Self {
            kind: "poll-result".into(),
            channel: Some(channel),
            events,
            ..Self::default()
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            kind: "error".into(),
            error: Some(message.into()),
            ..Self::default()
        }
    }
}

#[derive(Default)]
struct RelayState {
    clients: HashMap<String, ClientState>,
    subscriptions: HashMap<String, HashSet<String>>,
    queues: HashMap<String, VecDeque<QueuedEvent>>,
    pending_replies: HashMap<String, String>,
}

struct ClientState {
    sender: mpsc::UnboundedSender<Message>,
    role: String,
    client_id: String,
    channels: HashSet<String>,
}

#[derive(Clone)]
struct Outbound {
    sender: mpsc::UnboundedSender<Message>,
    frame: WireMessage,
}

type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEFAULT_BIND: &str = "127.0.0.1:9100";
const RENDERER_CHANNEL: &str = "renderer";

#[derive(Debug, Clone)]
struct RelayCliState {
    server: String,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve { bind } => {
            let bind = resolve_bind(bind)?;
            save_cli_state(&RelayCliState {
                server: server_url_from_bind(&bind),
            })?;
            run_server(bind).await
        }
        Command::Genui {
            server,
            layout,
            client_id,
            timeout_secs,
        } => run_genui(resolve_server(server)?, layout, client_id, timeout_secs).await,
        Command::Help {
            server,
            topics,
            client_id,
            timeout_secs,
        } => run_help(resolve_server(server)?, topics, client_id, timeout_secs).await,
        Command::Skill {
            server,
            client_id,
            timeout_secs,
        } => run_skill(resolve_server(server)?, client_id, timeout_secs).await,
        Command::Status {
            server,
            client_id,
            timeout_secs,
        } => run_status(resolve_server(server)?, client_id, timeout_secs).await,
        Command::Current => run_current(),
        Command::Open {
            server,
            client_id,
            timeout_secs,
        } => run_open(resolve_server(server)?, client_id, timeout_secs).await,
        Command::Send {
            server,
            channel,
            payload,
            client_id,
            timeout_secs,
        } => run_send(resolve_server(server)?, channel, payload, client_id, timeout_secs).await,
        Command::Poll {
            server,
            channel,
            limit,
            client_id,
        } => run_poll(resolve_server(server)?, channel, limit, client_id).await,
        Command::Reply {
            server,
            id,
            payload,
            error,
            client_id,
        } => run_reply(resolve_server(server)?, id, payload, error, client_id).await,
    }
}

async fn run_server(bind: String) -> Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    let state = Arc::new(Mutex::new(RelayState::default()));
    eprintln!("listening on ws://{bind}");

    loop {
        let (stream, addr) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(state, stream).await {
                eprintln!("connection {addr}: {error:#}");
            }
        });
    }
}

fn state_file_path() -> Result<PathBuf> {
    let home = env::var("HOME").map_err(|_| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".config").join("edn-relay.cirru"))
}

fn load_cli_state() -> Result<Option<RelayCliState>> {
    let path = state_file_path()?;
    if !path.exists() {
        return Ok(None);
    }

    let text = fs::read_to_string(&path)?;
    let edn = parse_edn_text(&text, "relay state file")?;
    let map = expect_map(edn, "relay state file")?;
    let server = required_map_string(&map, "server")?;
    Ok(Some(RelayCliState { server }))
}

fn save_cli_state(state: &RelayCliState) -> Result<()> {
    let path = state_file_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let edn = Edn::map_from_iter([(Edn::tag("server"), Edn::str(state.server.clone()))]);
    let text = cirru_edn::format(&edn, true)
        .map_err(|error| anyhow!("failed to format relay state file: {error}"))?;
    fs::write(path, text)?;
    Ok(())
}

fn server_url_from_bind(bind: &str) -> String {
    format!("ws://{bind}")
}

fn resolve_bind(bind: Option<String>) -> Result<String> {
    if let Some(bind) = bind {
        return Ok(bind);
    }

    if let Some(state) = load_cli_state()? {
        if let Some(bind) = state.server.strip_prefix("ws://") {
            return Ok(bind.to_string());
        }
    }

    Ok(DEFAULT_BIND.to_string())
}

fn resolve_server(server: Option<String>) -> Result<String> {
    if let Some(server) = server {
        return Ok(server);
    }

    if let Some(state) = load_cli_state()? {
        return Ok(state.server);
    }

    bail!("no relay target configured; run `edn-relay serve` first")
}

async fn run_send(
    server: String,
    channel: String,
    payload: String,
    client_id: Option<String>,
    timeout_secs: u64,
) -> Result<()> {
    let payload = parse_edn_text(&payload, "request payload")?;
    let ack =
        send_request_and_wait_for_ack(server, channel, payload, client_id, timeout_secs).await?;
    println!("{}", encode_frame(&ack)?);
    Ok(())
}

async fn run_genui(
    server: String,
    layout: String,
    client_id: Option<String>,
    timeout_secs: u64,
) -> Result<()> {
    let layout = parse_edn_text(&layout, "genui layout")?;
    validate_genui_layout(&layout)?;
    let ack =
        send_request_and_wait_for_ack(server, "genui".into(), layout, client_id, timeout_secs)
            .await?;

    if !ack.ok.unwrap_or(false) {
        bail!(
            ack.error
                .unwrap_or_else(|| "browser rejected the genui layout".into())
        );
    }

    let payload = ack
        .payload
        .ok_or_else(|| anyhow!("browser ack is missing genui payload"))?;
    let result: GenUiAckPayload = decode_edn(payload, "genui ack payload")?;
    if result.status != "ok" {
        bail!("unexpected genui ack status: {}", result.status);
    }

    println!("genui ok {}", result.layout_id);
    Ok(())
}

async fn run_help(
    server: String,
    topics: Vec<String>,
    client_id: Option<String>,
    timeout_secs: u64,
) -> Result<()> {
    let payload = Edn::map_from_iter([
        (Edn::tag("op"), Edn::str("help".to_owned())),
        (
            Edn::tag("topics"),
            Edn::List(EdnListView(topics.into_iter().map(Edn::str).collect())),
        ),
    ]);
    let ack = send_request_and_wait_for_ack(
        server,
        RENDERER_CHANNEL.into(),
        payload,
        client_id,
        timeout_secs,
    )
    .await?;
    print_renderer_response(ack)
}

async fn run_skill(
    server: String,
    client_id: Option<String>,
    timeout_secs: u64,
) -> Result<()> {
    let payload = Edn::map_from_iter([(Edn::tag("op"), Edn::str("skill".to_owned()))]);
    let ack = send_request_and_wait_for_ack(
        server,
        RENDERER_CHANNEL.into(),
        payload,
        client_id,
        timeout_secs,
    )
    .await?;
    print_renderer_response(ack)
}

async fn run_status(server: String, client_id: Option<String>, timeout_secs: u64) -> Result<()> {
    let status = fetch_renderer_status(server, client_id, timeout_secs).await?;
    print_renderer_status(&status)
}

fn run_current() -> Result<()> {
    let path = state_file_path()?;
    match load_cli_state()? {
        Some(state) => {
            println!("当前 relay 上下文");
            println!("  状态文件: {}", path.display());
            println!("  server: {}", state.server);
            Ok(())
        }
        None => {
            println!("当前 relay 上下文");
            println!("  状态文件: {}", path.display());
            println!("  尚未初始化；先运行 `edn-relay serve`");
            Ok(())
        }
    }
}

async fn run_open(server: String, client_id: Option<String>, timeout_secs: u64) -> Result<()> {
    let status = fetch_renderer_status(server, client_id, timeout_secs).await?;
    open_url(&status.page_url)?;
    println!("opened {}", status.page_url);
    Ok(())
}

async fn fetch_renderer_status(
    server: String,
    client_id: Option<String>,
    timeout_secs: u64,
) -> Result<RendererStatusPayload> {
    let payload = Edn::map_from_iter([(Edn::tag("op"), Edn::str("status".to_owned()))]);
    let ack = send_request_and_wait_for_ack(
        server,
        RENDERER_CHANNEL.into(),
        payload,
        client_id,
        timeout_secs,
    )
    .await?;

    if !ack.ok.unwrap_or(false) {
        bail!(
            ack.error
                .unwrap_or_else(|| "renderer returned an error".into())
        );
    }

    let payload = ack
        .payload
        .ok_or_else(|| anyhow!("renderer ack is missing payload"))?;
    renderer_status_from_payload(payload)
}

fn print_renderer_response(ack: WireMessage) -> Result<()> {
    if !ack.ok.unwrap_or(false) {
        bail!(
            ack.error
                .unwrap_or_else(|| "renderer returned an error".into())
        );
    }

    let payload = ack
        .payload
        .ok_or_else(|| anyhow!("renderer ack is missing payload"))?;

    if let Edn::Map(map) = payload.clone() {
        if matches!(map_string(&map, "kind")?.as_deref(), Some("help")) {
            print_renderer_help_payload(&map)?;
            return Ok(());
        }

        if let Some(text) = map_string(&map, "text")? {
            println!("{text}");
            return Ok(());
        }
    }

    println!(
        "{}",
        cirru_edn::format(&payload, true)
            .map_err(|error| anyhow!("failed to format renderer payload: {error}"))?
    );
    Ok(())
}

fn print_renderer_help_payload(map: &EdnMapView) -> Result<()> {
    let renderer = map_string(map, "renderer")?.unwrap_or_else(|| "renderer".into());
    let summary = map_string(map, "summary")?.unwrap_or_default();
    let commands = map_string_list(map, "commands")?.unwrap_or_default();
    let topics = map_string_list(map, "topics")?.unwrap_or_default();
    let components = map_components(map, "components")?;
    let protocol_docs = map_protocol_docs(map, "protocol_docs")?;
    let example_docs = map_example_docs(map, "examples")?;

    let mut output = String::new();
    writeln!(&mut output, "{renderer}")?;
    if !summary.is_empty() {
        writeln!(&mut output, "  {summary}")?;
    }

    if !commands.is_empty() {
        writeln!(&mut output, "")?;
        writeln!(&mut output, "可用命令:")?;
        for command in commands {
            writeln!(&mut output, "  - edn-relay {command}")?;
        }
    }

    let mut has_section = false;
    if !components.is_empty() {
        writeln!(&mut output, "")?;
        if topics.is_empty() {
            writeln!(&mut output, "组件说明:")?;
        } else {
            writeln!(&mut output, "组件说明(筛选: {}):", topics.join(", "))?;
        }
        for component in components {
            writeln!(&mut output, "")?;
            writeln!(&mut output, "- {}", component.name)?;
            if !component.summary.is_empty() {
                writeln!(&mut output, "  {}", component.summary)?;
            }
            if !component.fields.is_empty() {
                writeln!(&mut output, "  字段: {}", component.fields.join(", "))?;
            }
            if !component.example.is_empty() {
                writeln!(&mut output, "  示例:")?;
                for line in component.example.lines() {
                    writeln!(&mut output, "    {line}")?;
                }
            }
        }
        has_section = true;
    }

    if !protocol_docs.is_empty() {
        writeln!(&mut output, "")?;
        writeln!(&mut output, "协议摘要:")?;
        for item in protocol_docs {
            writeln!(&mut output, "  - {}: {}", item.name, item.summary)?;
        }
        has_section = true;
    }

    if !example_docs.is_empty() {
        writeln!(&mut output, "")?;
        writeln!(&mut output, "示例:")?;
        for item in example_docs {
            writeln!(&mut output, "")?;
            writeln!(&mut output, "- {}", item.name)?;
            if !item.summary.is_empty() {
                writeln!(&mut output, "  {}", item.summary)?;
            }
            if !item.payload.is_empty() {
                writeln!(&mut output, "  payload:")?;
                for line in item.payload.lines() {
                    writeln!(&mut output, "    {line}")?;
                }
            }
        }
        has_section = true;
    }

    if !has_section {
        writeln!(&mut output, "")?;
        writeln!(&mut output, "没有匹配的帮助主题。")?;
    }

    print!("{output}");
    Ok(())
}

#[derive(Debug)]
struct RendererComponentDoc {
    name: String,
    summary: String,
    fields: Vec<String>,
    example: String,
}

#[derive(Debug)]
struct RendererProtocolDoc {
    name: String,
    summary: String,
}

#[derive(Debug)]
struct RendererExampleDoc {
    name: String,
    summary: String,
    payload: String,
}

#[derive(Debug)]
struct RendererStatusPayload {
    renderer: String,
    title: String,
    page_url: String,
    commands: Vec<String>,
}

fn map_components(map: &EdnMapView, key: &str) -> Result<Vec<RendererComponentDoc>> {
    match map_value(map, key) {
        Some(Edn::Nil) | None => Ok(Vec::new()),
        Some(Edn::List(EdnListView(items))) => items
            .iter()
            .cloned()
            .map(component_doc_from_edn)
            .collect::<Result<Vec<_>>>(),
        Some(other) => bail!("field `{key}` must be a list, got {other}"),
    }
}

fn map_protocol_docs(map: &EdnMapView, key: &str) -> Result<Vec<RendererProtocolDoc>> {
    match map_value(map, key) {
        Some(Edn::Nil) | None => Ok(Vec::new()),
        Some(Edn::List(EdnListView(items))) => items
            .iter()
            .cloned()
            .map(protocol_doc_from_edn)
            .collect::<Result<Vec<_>>>(),
        Some(other) => bail!("field `{key}` must be a list, got {other}"),
    }
}

fn map_example_docs(map: &EdnMapView, key: &str) -> Result<Vec<RendererExampleDoc>> {
    match map_value(map, key) {
        Some(Edn::Nil) | None => Ok(Vec::new()),
        Some(Edn::List(EdnListView(items))) => items
            .iter()
            .cloned()
            .map(example_doc_from_edn)
            .collect::<Result<Vec<_>>>(),
        Some(other) => bail!("field `{key}` must be a list, got {other}"),
    }
}

fn component_doc_from_edn(edn: Edn) -> Result<RendererComponentDoc> {
    let map = expect_map(edn, "renderer component doc")?;
    Ok(RendererComponentDoc {
        name: required_map_string(&map, "name")?,
        summary: map_string(&map, "summary")?.unwrap_or_default(),
        fields: map_string_list(&map, "fields")?.unwrap_or_default(),
        example: map_string(&map, "example")?.unwrap_or_default(),
    })
}

fn protocol_doc_from_edn(edn: Edn) -> Result<RendererProtocolDoc> {
    let map = expect_map(edn, "renderer protocol doc")?;
    Ok(RendererProtocolDoc {
        name: required_map_string(&map, "name")?,
        summary: map_string(&map, "summary")?.unwrap_or_default(),
    })
}

fn example_doc_from_edn(edn: Edn) -> Result<RendererExampleDoc> {
    let map = expect_map(edn, "renderer example doc")?;
    Ok(RendererExampleDoc {
        name: required_map_string(&map, "name")?,
        summary: map_string(&map, "summary")?.unwrap_or_default(),
        payload: map_string(&map, "payload")?.unwrap_or_default(),
    })
}

fn renderer_status_from_payload(payload: Edn) -> Result<RendererStatusPayload> {
    let map = expect_map(payload, "renderer status payload")?;
    let kind = required_map_string(&map, "kind")?;
    if kind != "status" {
        bail!("unexpected renderer payload kind for status: {kind}");
    }

    Ok(RendererStatusPayload {
        renderer: required_map_string(&map, "renderer")?,
        title: map_string(&map, "title")?.unwrap_or_default(),
        page_url: required_map_string(&map, "page_url")?,
        commands: map_string_list(&map, "commands")?.unwrap_or_default(),
    })
}

fn print_renderer_status(status: &RendererStatusPayload) -> Result<()> {
    println!("{}", status.renderer);
    if !status.title.is_empty() {
        println!("  title: {}", status.title);
    }
    println!("  page: {}", status.page_url);
    if !status.commands.is_empty() {
        println!("  commands: {}", status.commands.join(", "));
    }
    Ok(())
}

fn open_url(url: &str) -> Result<()> {
    if cfg!(target_os = "macos") {
        ProcessCommand::new("open").arg(url).spawn()?;
        return Ok(());
    }

    if cfg!(target_os = "linux") {
        ProcessCommand::new("xdg-open").arg(url).spawn()?;
        return Ok(());
    }

    if cfg!(target_os = "windows") {
        ProcessCommand::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()?;
        return Ok(());
    }

    bail!("open is not supported on this platform")
}

async fn send_request_and_wait_for_ack(
    server: String,
    channel: String,
    payload: Edn,
    client_id: Option<String>,
    timeout_secs: u64,
) -> Result<WireMessage> {
    let mut socket = connect_client(&server).await?;
    let request_id = Uuid::new_v4().to_string();

    send_client_frame(
        &mut socket,
        &WireMessage::hello("cli", client_id, Vec::new()),
    )
    .await?;
    send_client_frame(
        &mut socket,
        &WireMessage::request(request_id.clone(), channel, payload),
    )
    .await?;

    let ack = timeout(Duration::from_secs(timeout_secs), async {
        loop {
            let Some(frame) = read_client_frame(&mut socket).await? else {
                bail!("server closed the connection before returning an ack");
            };

            match frame.kind.as_str() {
                "hello-ok" | "accepted" => continue,
                "ack" if frame.id.as_deref() == Some(request_id.as_str()) => return Ok(frame),
                "error" => {
                    let message = frame
                        .error
                        .unwrap_or_else(|| "server returned an error".into());
                    bail!(message);
                }
                _ => continue,
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for ack for request {}", request_id))??;

    Ok(ack)
}

async fn run_poll(
    server: String,
    channel: String,
    limit: usize,
    client_id: Option<String>,
) -> Result<()> {
    let mut socket = connect_client(&server).await?;
    send_client_frame(
        &mut socket,
        &WireMessage::hello("worker", client_id, Vec::new()),
    )
    .await?;
    send_client_frame(
        &mut socket,
        &WireMessage::poll(channel.clone(), limit.max(1)),
    )
    .await?;

    loop {
        let Some(frame) = read_client_frame(&mut socket).await? else {
            bail!("server closed the connection before returning poll results");
        };

        match frame.kind.as_str() {
            "hello-ok" => continue,
            "poll-result" if frame.channel.as_deref() == Some(channel.as_str()) => {
                println!("{}", encode_frame(&frame)?);
                return Ok(());
            }
            "error" => {
                let message = frame
                    .error
                    .unwrap_or_else(|| "server returned an error".into());
                bail!(message);
            }
            _ => continue,
        }
    }
}

async fn run_reply(
    server: String,
    id: String,
    payload: Option<String>,
    error: Option<String>,
    client_id: Option<String>,
) -> Result<()> {
    let payload = match payload {
        Some(payload) => Some(parse_edn_text(&payload, "reply payload")?),
        None => None,
    };

    let ok = error.is_none();
    let mut socket = connect_client(&server).await?;
    send_client_frame(
        &mut socket,
        &WireMessage::hello("cli", client_id, Vec::new()),
    )
    .await?;
    send_client_frame(
        &mut socket,
        &WireMessage::ack(id.clone(), ok, payload, error),
    )
    .await?;

    loop {
        let Some(frame) = read_client_frame(&mut socket).await? else {
            bail!("server closed the connection before confirming the reply");
        };

        match frame.kind.as_str() {
            "hello-ok" => continue,
            "reply-accepted" if frame.id.as_deref() == Some(id.as_str()) => {
                println!("{}", encode_frame(&frame)?);
                return Ok(());
            }
            "error" => {
                let message = frame
                    .error
                    .unwrap_or_else(|| "server returned an error".into());
                bail!(message);
            }
            _ => continue,
        }
    }
}

async fn connect_client(server: &str) -> Result<ClientSocket> {
    let (socket, _) = connect_async(server).await?;
    Ok(socket)
}

async fn send_client_frame(socket: &mut ClientSocket, frame: &WireMessage) -> Result<()> {
    socket.send(frame_as_text(frame)?).await?;
    Ok(())
}

async fn read_client_frame(socket: &mut ClientSocket) -> Result<Option<WireMessage>> {
    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => return Ok(Some(decode_frame(&text)?)),
            Message::Binary(_) => bail!("received an unexpected binary websocket frame"),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Pong(_) => continue,
            Message::Close(_) => return Ok(None),
            Message::Frame(_) => continue,
        }
    }

    Ok(None)
}

async fn handle_connection(state: Arc<Mutex<RelayState>>, stream: TcpStream) -> Result<()> {
    let socket = accept_async(stream).await?;
    let (mut sink, mut stream) = socket.split();
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();
    let session_id = Uuid::new_v4().to_string();

    {
        let mut state = state.lock().await;
        state.clients.insert(
            session_id.clone(),
            ClientState {
                sender: sender.clone(),
                role: "unknown".into(),
                client_id: session_id.clone(),
                channels: HashSet::new(),
            },
        );
    }

    let writer = tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });

    while let Some(message) = stream.next().await {
        match message? {
            Message::Text(text) => match decode_frame(&text) {
                Ok(frame) => match process_server_frame(&state, &session_id, frame).await {
                    Ok(actions) => {
                        for action in actions {
                            let _ = dispatch(action);
                        }
                    }
                    Err(error) => {
                        let _ = send_direct_error(&sender, error.to_string());
                    }
                },
                Err(error) => {
                    let _ = send_direct_error(&sender, error.to_string());
                }
            },
            Message::Binary(_) => {
                let _ = send_direct_error(&sender, "binary websocket frames are not supported");
            }
            Message::Ping(payload) => {
                let _ = sender.send(Message::Pong(payload));
            }
            Message::Pong(_) => continue,
            Message::Close(_) => break,
            Message::Frame(_) => continue,
        }
    }

    cleanup_connection(&state, &session_id).await;
    writer.abort();
    Ok(())
}

async fn process_server_frame(
    state: &Arc<Mutex<RelayState>>,
    session_id: &str,
    frame: WireMessage,
) -> Result<Vec<Outbound>> {
    match frame.kind.as_str() {
        "hello" => process_hello(state, session_id, frame).await,
        "request" => process_request(state, session_id, frame).await,
        "poll" => process_poll(state, session_id, frame).await,
        "ack" => process_ack(state, session_id, frame).await,
        other => {
            let message = format!("unsupported protocol message kind: {other}");
            let sender = current_sender(state, session_id).await?;
            Ok(vec![Outbound {
                sender,
                frame: WireMessage::error(message),
            }])
        }
    }
}

async fn process_hello(
    state: &Arc<Mutex<RelayState>>,
    session_id: &str,
    frame: WireMessage,
) -> Result<Vec<Outbound>> {
    let role = required_field(frame.role, "role")?;
    if !matches!(role.as_str(), "browser" | "cli" | "worker") {
        bail!("invalid role: {role}");
    }

    let resolved_client_id = frame.client_id.unwrap_or_else(|| session_id.to_string());
    let requested_channels: HashSet<String> = frame
        .channels
        .into_iter()
        .filter(|channel| !channel.is_empty())
        .collect();

    let (sender, queued_events) = {
        let mut state = state.lock().await;
        let sender = state
            .clients
            .get(session_id)
            .map(|client| client.sender.clone())
            .ok_or_else(|| anyhow!("client session is missing"))?;

        let previous_channels = state
            .clients
            .get(session_id)
            .map(|client| client.channels.clone())
            .unwrap_or_default();

        let mut empty_subscriptions = Vec::new();
        for channel in &previous_channels {
            if let Some(members) = state.subscriptions.get_mut(channel) {
                members.remove(session_id);
                if members.is_empty() {
                    empty_subscriptions.push(channel.clone());
                }
            }
        }
        for channel in empty_subscriptions {
            state.subscriptions.remove(&channel);
        }

        if let Some(client) = state.clients.get_mut(session_id) {
            client.role = role;
            client.client_id = resolved_client_id.clone();
            client.channels = requested_channels.clone();
        }

        for channel in &requested_channels {
            state
                .subscriptions
                .entry(channel.clone())
                .or_default()
                .insert(session_id.to_string());
        }

        let mut queued_events = Vec::new();
        let mut empty_queues = Vec::new();
        for channel in &requested_channels {
            if let Some(queue) = state.queues.get_mut(channel) {
                while let Some(event) = queue.pop_front() {
                    queued_events.push(event);
                }
                if queue.is_empty() {
                    empty_queues.push(channel.clone());
                }
            }
        }
        for channel in empty_queues {
            state.queues.remove(&channel);
        }

        (sender, queued_events)
    };

    let mut actions = vec![Outbound {
        sender: sender.clone(),
        frame: WireMessage::hello_ok(resolved_client_id),
    }];
    for event in queued_events {
        actions.push(Outbound {
            sender: sender.clone(),
            frame: WireMessage::event(event),
        });
    }
    Ok(actions)
}

async fn process_request(
    state: &Arc<Mutex<RelayState>>,
    session_id: &str,
    frame: WireMessage,
) -> Result<Vec<Outbound>> {
    let id = required_field(frame.id, "id")?;
    let channel = required_field(frame.channel, "channel")?;
    let payload = required_value(frame.payload, "payload")?;

    let expects_reply = frame.expects_reply.unwrap_or(true);
    let (requester_sender, requester_id, recipients) = {
        let state = state.lock().await;
        let requester_sender = state
            .clients
            .get(session_id)
            .map(|client| client.sender.clone())
            .ok_or_else(|| anyhow!("client session is missing"))?;
        let requester_id = state
            .clients
            .get(session_id)
            .map(|client| client.client_id.clone())
            .unwrap_or_else(|| session_id.to_string());
        let recipients = state
            .subscriptions
            .get(&channel)
            .into_iter()
            .flat_map(|members| members.iter())
            .filter(|member| member.as_str() != session_id)
            .filter_map(|member| {
                state
                    .clients
                    .get(member)
                    .map(|client| client.sender.clone())
            })
            .collect::<Vec<_>>();
        (requester_sender, requester_id, recipients)
    };

    let event = QueuedEvent {
        id: id.clone(),
        channel: channel.clone(),
        from: requester_id,
        payload,
    };

    let status = if recipients.is_empty() {
        let mut state = state.lock().await;
        if expects_reply {
            state
                .pending_replies
                .insert(id.clone(), session_id.to_string());
        }
        state
            .queues
            .entry(channel.clone())
            .or_default()
            .push_back(event.clone());
        "queued"
    } else {
        let mut state = state.lock().await;
        if expects_reply {
            state
                .pending_replies
                .insert(id.clone(), session_id.to_string());
        }
        "delivered"
    };

    let mut actions = vec![Outbound {
        sender: requester_sender,
        frame: WireMessage::accepted(id, channel.clone(), status),
    }];
    for recipient in recipients {
        actions.push(Outbound {
            sender: recipient,
            frame: WireMessage::event(event.clone()),
        });
    }
    Ok(actions)
}

async fn process_poll(
    state: &Arc<Mutex<RelayState>>,
    session_id: &str,
    frame: WireMessage,
) -> Result<Vec<Outbound>> {
    let channel = required_field(frame.channel, "channel")?;
    let limit = frame.limit.unwrap_or(1).max(1);

    let (sender, events) = {
        let mut state = state.lock().await;
        let sender = state
            .clients
            .get(session_id)
            .map(|client| client.sender.clone())
            .ok_or_else(|| anyhow!("client session is missing"))?;

        let mut events = Vec::new();
        let mut should_remove = false;
        if let Some(queue) = state.queues.get_mut(&channel) {
            for _ in 0..limit {
                match queue.pop_front() {
                    Some(event) => events.push(event),
                    None => break,
                }
            }
            should_remove = queue.is_empty();
        }
        if should_remove {
            state.queues.remove(&channel);
        }

        (sender, events)
    };

    Ok(vec![Outbound {
        sender,
        frame: WireMessage::poll_result(channel, events),
    }])
}

async fn process_ack(
    state: &Arc<Mutex<RelayState>>,
    session_id: &str,
    frame: WireMessage,
) -> Result<Vec<Outbound>> {
    let id = required_field(frame.id, "id")?;
    let ok = frame.ok.unwrap_or(frame.error.is_none());
    let sender = current_sender(state, session_id).await?;
    let requester_sender = {
        let mut state = state.lock().await;
        let Some(requester_id) = state.pending_replies.remove(&id) else {
            return Ok(vec![Outbound {
                sender,
                frame: WireMessage::error(format!("request {id} is not waiting for a reply")),
            }]);
        };

        state
            .clients
            .get(&requester_id)
            .map(|client| client.sender.clone())
    };

    let Some(requester_sender) = requester_sender else {
        return Ok(vec![Outbound {
            sender,
            frame: WireMessage::error(format!("requester for {id} is no longer connected")),
        }]);
    };

    Ok(vec![
        Outbound {
            sender: requester_sender,
            frame: WireMessage::ack(id.clone(), ok, frame.payload, frame.error),
        },
        Outbound {
            sender,
            frame: WireMessage::reply_accepted(id),
        },
    ])
}

async fn current_sender(
    state: &Arc<Mutex<RelayState>>,
    session_id: &str,
) -> Result<mpsc::UnboundedSender<Message>> {
    let state = state.lock().await;
    state
        .clients
        .get(session_id)
        .map(|client| client.sender.clone())
        .ok_or_else(|| anyhow!("client session is missing"))
}

async fn cleanup_connection(state: &Arc<Mutex<RelayState>>, session_id: &str) {
    let mut state = state.lock().await;
    let Some(client) = state.clients.remove(session_id) else {
        return;
    };

    let mut empty_subscriptions = Vec::new();
    for channel in &client.channels {
        if let Some(members) = state.subscriptions.get_mut(channel) {
            members.remove(session_id);
            if members.is_empty() {
                empty_subscriptions.push(channel.clone());
            }
        }
    }
    for channel in empty_subscriptions {
        state.subscriptions.remove(&channel);
    }

    state
        .pending_replies
        .retain(|_, waiter| waiter != session_id);
}

fn dispatch(outbound: Outbound) -> Result<()> {
    outbound
        .sender
        .send(frame_as_text(&outbound.frame)?)
        .map_err(|_| {
            anyhow!("failed to send websocket frame because the target connection is closed")
        })?;
    Ok(())
}

fn send_direct_error(
    sender: &mpsc::UnboundedSender<Message>,
    message: impl Into<String>,
) -> Result<()> {
    sender
        .send(frame_as_text(&WireMessage::error(message))?)
        .map_err(|_| {
            anyhow!("failed to send websocket error because the target connection is closed")
        })?;
    Ok(())
}

fn frame_as_text(frame: &WireMessage) -> Result<Message> {
    Ok(Message::Text(encode_frame(frame)?.into()))
}

fn encode_frame(frame: &WireMessage) -> Result<String> {
    let edn = wire_message_to_edn(frame);
    cirru_edn::format(&edn, true)
        .map_err(|error| anyhow!("failed to format Cirru EDN protocol frame: {error}"))
}

fn decode_frame(text: &str) -> Result<WireMessage> {
    let edn = parse_edn_text(text, "protocol frame")?;
    wire_message_from_edn(edn)
}

fn validate_genui_layout(layout: &Edn) -> Result<()> {
    let root: GenUiLayoutNode = decode_edn(layout.clone(), "genui layout")?;
    validate_genui_node(&root, "root")
}

fn validate_genui_node(node: &GenUiLayoutNode, path: &str) -> Result<()> {
    match node.node_type.as_str() {
        "column" | "row" | "card" => {
            for (index, child) in node.children.iter().enumerate() {
                validate_genui_node(child, &format!("{path}.children[{index}]"))?;
            }
            Ok(())
        }
        "text" => require_string_field(path, "text", node.text.as_deref()),
        "badge" => require_string_field(path, "text", node.text.as_deref()),
        "divider" => Ok(()),
        "button" => require_string_field(path, "text", node.text.as_deref()),
        "markdown" => require_string_field(path, "text", node.text.as_deref()),
        "mermaid" => require_string_field(path, "text", node.text.as_deref()),
        "chart" => validate_chart_series(&node.series, path),
        "input" => {
            if node.name.as_deref().is_none() && node.placeholder.as_deref().is_none() {
                bail!("{path} input node requires at least one of `name` or `placeholder`");
            }
            Ok(())
        }
        other => bail!("{path} has unsupported node type `{other}`"),
    }
}

fn require_string_field(path: &str, field_name: &str, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) if !value.is_empty() => Ok(()),
        _ => bail!("{path} requires a non-empty `{field_name}` field"),
    }
}

fn validate_chart_series(series: &[GenUiChartItem], path: &str) -> Result<()> {
    if series.is_empty() {
        bail!("{path} chart node requires non-empty `series`");
    }

    for (index, item) in series.iter().enumerate() {
        if item.label.is_empty() {
            bail!("{path}.series[{index}] requires non-empty `label`");
        }
        if !item.value.is_finite() {
            bail!("{path}.series[{index}] requires finite `value`");
        }
    }

    Ok(())
}

fn parse_edn_text(text: &str, label: &str) -> Result<Edn> {
    cirru_edn::parse(text).map_err(|error| anyhow!("failed to parse Cirru EDN {label}: {error}"))
}

fn decode_edn<T>(edn: Edn, label: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    cirru_edn::from_edn(edn).map_err(|error| anyhow!("failed to decode Cirru EDN {label}: {error}"))
}

fn wire_message_to_edn(frame: &WireMessage) -> Edn {
    let mut pairs = Vec::new();
    pairs.push((Edn::tag("kind"), Edn::str(frame.kind.clone())));

    if let Some(id) = &frame.id {
        pairs.push((Edn::tag("id"), Edn::str(id.clone())));
    }
    if let Some(role) = &frame.role {
        pairs.push((Edn::tag("role"), Edn::str(role.clone())));
    }
    if let Some(client_id) = &frame.client_id {
        pairs.push((Edn::tag("client_id"), Edn::str(client_id.clone())));
    }
    if !frame.channels.is_empty() {
        pairs.push((
            Edn::tag("channels"),
            Edn::List(EdnListView(
                frame
                    .channels
                    .iter()
                    .cloned()
                    .map(Edn::str)
                    .collect::<Vec<_>>(),
            )),
        ));
    }
    if let Some(channel) = &frame.channel {
        pairs.push((Edn::tag("channel"), Edn::str(channel.clone())));
    }
    if let Some(expects_reply) = frame.expects_reply {
        pairs.push((Edn::tag("expects_reply"), Edn::Bool(expects_reply)));
    }
    if let Some(ok) = frame.ok {
        pairs.push((Edn::tag("ok"), Edn::Bool(ok)));
    }
    if let Some(payload) = &frame.payload {
        pairs.push((Edn::tag("payload"), payload.clone()));
    }
    if let Some(error) = &frame.error {
        pairs.push((Edn::tag("error"), Edn::str(error.clone())));
    }
    if let Some(limit) = frame.limit {
        pairs.push((Edn::tag("limit"), Edn::Number(limit as f64)));
    }
    if !frame.events.is_empty() {
        pairs.push((
            Edn::tag("events"),
            Edn::List(EdnListView(
                frame
                    .events
                    .iter()
                    .map(queued_event_to_edn)
                    .collect::<Vec<_>>(),
            )),
        ));
    }
    if let Some(from) = &frame.from {
        pairs.push((Edn::tag("from"), Edn::str(from.clone())));
    }
    if let Some(status) = &frame.status {
        pairs.push((Edn::tag("status"), Edn::str(status.clone())));
    }

    Edn::map_from_iter(pairs)
}

fn queued_event_to_edn(event: &QueuedEvent) -> Edn {
    Edn::map_from_iter([
        (Edn::tag("id"), Edn::str(event.id.clone())),
        (Edn::tag("channel"), Edn::str(event.channel.clone())),
        (Edn::tag("from"), Edn::str(event.from.clone())),
        (Edn::tag("payload"), event.payload.clone()),
    ])
}

fn wire_message_from_edn(edn: Edn) -> Result<WireMessage> {
    let map = expect_map(edn, "protocol frame")?;

    Ok(WireMessage {
        kind: required_map_string(&map, "kind")?,
        id: map_string(&map, "id")?,
        role: map_string(&map, "role")?,
        client_id: map_string(&map, "client_id")?,
        channels: map_string_list(&map, "channels")?.unwrap_or_default(),
        channel: map_string(&map, "channel")?,
        expects_reply: map_bool(&map, "expects_reply")?,
        ok: map_bool(&map, "ok")?,
        payload: map_edn(&map, "payload"),
        error: map_string(&map, "error")?,
        limit: map_usize(&map, "limit")?,
        events: map_events(&map, "events")?.unwrap_or_default(),
        from: map_string(&map, "from")?,
        status: map_string(&map, "status")?,
    })
}

fn queued_event_from_edn(edn: Edn) -> Result<QueuedEvent> {
    let map = expect_map(edn, "queued event")?;

    Ok(QueuedEvent {
        id: required_map_string(&map, "id")?,
        channel: required_map_string(&map, "channel")?,
        from: required_map_string(&map, "from")?,
        payload: required_value(map_edn(&map, "payload"), "payload")?,
    })
}

fn expect_map(edn: Edn, label: &str) -> Result<EdnMapView> {
    match edn {
        Edn::Map(map) => Ok(map),
        other => bail!("{label} must be an EDN map, got {other}"),
    }
}

fn map_value<'a>(map: &'a EdnMapView, key: &str) -> Option<&'a Edn> {
    map.tag_get(key).or_else(|| map.str_get(key))
}

fn map_string(map: &EdnMapView, key: &str) -> Result<Option<String>> {
    match map_value(map, key) {
        Some(Edn::Nil) => Ok(None),
        Some(value) => Ok(Some(edn_as_string(value, key)?)),
        None => Ok(None),
    }
}

fn required_map_string(map: &EdnMapView, key: &str) -> Result<String> {
    required_field(map_string(map, key)?, key)
}

fn map_bool(map: &EdnMapView, key: &str) -> Result<Option<bool>> {
    match map_value(map, key) {
        Some(Edn::Nil) => Ok(None),
        Some(Edn::Bool(value)) => Ok(Some(*value)),
        Some(other) => bail!("field `{key}` must be a boolean, got {other}"),
        None => Ok(None),
    }
}

fn map_usize(map: &EdnMapView, key: &str) -> Result<Option<usize>> {
    match map_value(map, key) {
        Some(Edn::Nil) => Ok(None),
        Some(Edn::Number(value)) if *value >= 0.0 && value.fract().abs() < f64::EPSILON => {
            Ok(Some(*value as usize))
        }
        Some(other) => bail!("field `{key}` must be a non-negative integer, got {other}"),
        None => Ok(None),
    }
}

fn map_edn(map: &EdnMapView, key: &str) -> Option<Edn> {
    match map_value(map, key) {
        Some(Edn::Nil) | None => None,
        Some(value) => Some(value.clone()),
    }
}

fn map_string_list(map: &EdnMapView, key: &str) -> Result<Option<Vec<String>>> {
    match map_value(map, key) {
        Some(Edn::Nil) => Ok(None),
        Some(Edn::List(EdnListView(items))) => items
            .iter()
            .map(|item| edn_as_string(item, key))
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(other) => bail!("field `{key}` must be a list, got {other}"),
        None => Ok(None),
    }
}

fn map_events(map: &EdnMapView, key: &str) -> Result<Option<Vec<QueuedEvent>>> {
    match map_value(map, key) {
        Some(Edn::Nil) => Ok(None),
        Some(Edn::List(EdnListView(items))) => items
            .iter()
            .cloned()
            .map(queued_event_from_edn)
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(other) => bail!("field `{key}` must be a list, got {other}"),
        None => Ok(None),
    }
}

fn edn_as_string(value: &Edn, field_name: &str) -> Result<String> {
    match value {
        Edn::Str(value) => Ok(value.as_ref().to_owned()),
        Edn::Tag(value) => Ok(value.ref_str().to_owned()),
        other => bail!("field `{field_name}` must be a string, got {other}"),
    }
}

fn required_field(value: Option<String>, field_name: &str) -> Result<String> {
    match value {
        Some(value) if !value.is_empty() => Ok(value),
        _ => bail!("missing required field: {field_name}"),
    }
}

fn required_value<T>(value: Option<T>, field_name: &str) -> Result<T> {
    value.ok_or_else(|| anyhow!("missing required field: {field_name}"))
}
