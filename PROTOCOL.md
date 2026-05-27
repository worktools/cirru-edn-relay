# Cirru EDN Relay 协议

本文档定义 `cirru_edn_relay` 使用的 websocket 文本协议。所有消息都必须使用 Cirru EDN 编码，且 websocket 只发送文本帧，不发送二进制帧。

## 1. 总体约定

- websocket 地址示例: `ws://127.0.0.1:9100`
- 每一帧都是一个 Cirru EDN map，对应 Rust 侧的 `WireMessage`
- 顶层字段名固定，采用 snake_case
- `payload` 字段直接承载 Cirru EDN 数据，不再把数据额外包一层字符串
- `channel` 用于路由消息，同一个 channel 可以有多个 sender、receiver 或 worker
- 只要某个 channel 里还有任意一方在线，该 channel 就视为存在；所有连接退出后该 channel 消失
- `id` 是一次请求-回执往返的唯一标识，建议由发送方生成 UUID
- 服务端收到 `request` 后，若当前没有在线订阅者，则进入队列；后续可被订阅连接自动取到，或者由 `poll` 主动拉取

## 2. 连接初始化

任何客户端连上 websocket 后，第一条业务消息都应该先发 `hello`。

### hello

字段说明:

- `kind`: 固定为 `"hello"`
- `role`: `"receiver"`、`"sender"`、`"worker"`；当前仍兼容旧值 `"browser"` 和 `"cli"`
- `client_id`: 可选，客户端自定义标识；不传时由服务端回填 session id
- `channels`: 可选数组，表示当前连接当前要加入的频道列表；browser/receiver 通常只放 0 或 1 个

示例:

```cirru
{}
  :kind |hello
  :role |receiver
  :client_id |page-main
  :channels $ [] |demo
```

### hello-ok

服务端确认握手成功后返回:

- `kind`: 固定为 `"hello-ok"`
- `client_id`: 服务端最终确认的连接标识
- `channels`: 当前活跃的 channel 列表

示例:

```cirru
{}
  :kind |hello-ok
  :client_id |page-main
  :channels $ [] |demo
```

### channel-state

当活跃 channel 列表变化时，服务端会向在线连接广播:

- `kind`: 固定为 `"channel-state"`
- `channels`: 当前活跃的 channel 列表

示例:

```cirru
{}
  :kind |channel-state
  :channels $ [] |alpha |beta
```

## 3. 请求与回执

### request

由命令行发送方或任意生产者发送。

字段说明:

- `kind`: 固定为 `"request"`
- `id`: 请求 id，后续 `ack` 必须回同一个 id
- `channel`: 路由频道
- `payload`: 一段合法的 Cirru EDN 数据
- `expects_reply`: 当前实现默认 `true`

示例:

```cirru
{}
  :kind |request
  :id |0a94a0b0-8e6b-46dd-9ab4-1f6e0f88d8c7
  :channel |demo
  :expects_reply true
  :payload $ {} (:op |ping) (:value 1)
```

### accepted

服务端接收 `request` 后立即返回给发送方。

字段说明:

- `kind`: 固定为 `"accepted"`
- `id`: 原请求 id
- `channel`: 原 channel
- `status`: `"delivered"` 或 `"queued"`

语义:

- `delivered`: 当前有在线订阅者，事件已实时下发
- `queued`: 当前没有在线订阅者，事件已进入服务端队列

### event

服务端投递给浏览器或 worker 的事件。

字段说明:

- `kind`: 固定为 `"event"`
- `id`: 原请求 id
- `channel`: 原 channel
- `from`: 发送方 `client_id`
- `payload`: 原样透传的 Cirru EDN 数据

示例:

```cirru
{}
  :kind |event
  :id |0a94a0b0-8e6b-46dd-9ab4-1f6e0f88d8c7
  :channel |demo
  :from |cli-a
  :payload $ {} (:op |ping) (:value 1)
```

### ack

消费者处理完 `event` 后发回给服务端，服务端再按 `id` 转发给最初的发送方。

字段说明:

- `kind`: 固定为 `"ack"`
- `id`: 原请求 id
- `ok`: 是否成功
- `payload`: 可选，一段合法的 Cirru EDN 数据
- `error`: 可选，失败时的错误描述

约定:

- 成功回执: `ok = true`，可附带 `payload`
- 失败回执: `ok = false`，建议附带 `error`
- 同一个 `id` 只接受第一条有效回执，后续重复回执不会再转发给 sender，而是给晚到的 responder 返回 `warning`

成功回执示例:

```cirru
{}
  :kind |ack
  :id |0a94a0b0-8e6b-46dd-9ab4-1f6e0f88d8c7
  :ok true
  :payload $ {} (:result |pong)
```

失败回执示例:

```cirru
{}
  :kind |ack
  :id |0a94a0b0-8e6b-46dd-9ab4-1f6e0f88d8c7
  :ok false
  :error |permission-denied
```

### reply-accepted

服务端已成功把 `ack` 路由给原发送方时，返回给回执提交者:

- `kind`: 固定为 `"reply-accepted"`
- `id`: 已确认路由的请求 id

### warning

服务端在不需要中断连接、但需要提示行为被忽略时返回:

- `kind`: 固定为 `"warning"`
- `error`: 警告文本

目前主要用于多 receiver 场景下的重复 `ack`。

## 4. 队列拉取

### poll

worker 或命令行可以主动从服务端拉取队列里的事件。

字段说明:

- `kind`: 固定为 `"poll"`
- `channel`: 要拉取的频道
- `limit`: 最多返回多少条，最小按 1 处理

示例:

```cirru
{}
  :kind |poll
  :channel |demo
  :limit 10
```

### poll-result

服务端返回队列结果。

字段说明:

- `kind`: 固定为 `"poll-result"`
- `channel`: 拉取的频道
- `events`: 数组，每个元素都等价于一个 `event` 载荷

示例:

```cirru
{}
  :kind |poll-result
  :channel |demo
  :events $ [] $ {}
    :id |0a94a0b0-8e6b-46dd-9ab4-1f6e0f88d8c7
    :channel |demo
    :from |cli-a
    :payload $ {} (:op |ping) (:value 1)
```

注意:

- `poll` 会把事件从服务端队列中弹出
- 如果 `poll` 后没有发送 `ack`，原始发送方会持续等待直到超时或断开
- 因此 `poll` 适合接 worker 进程，worker 拉取后应自行实现 websocket 回执

## 5. 错误帧

### error

服务端遇到协议错误、字段缺失、回执 id 无效等情况时返回:

- `kind`: 固定为 `"error"`
- `error`: 错误描述文本

示例:

```cirru
{}
  :kind |error
  :error |missing-required-field-channel
```

## 6. 推荐交互顺序

### 6.1 浏览器常驻订阅

1. 建立 websocket 连接
2. 发送 `hello(role=receiver, channels=[当前选中的 channel])`
3. 等待 `hello-ok` 和后续的 `channel-state`
4. 持续接收 `event`
5. 处理完成后发送 `ack`

### 6.2 命令行发送并等待回执

1. 建立 websocket 连接
2. 发送 `hello(role=sender, channels=[本次命令显式传入的 channel])`
3. 发送 `request`
4. 收到 `accepted`
5. 等待服务端转发回来的第一条 `ack`

### 6.3 worker 拉取队列任务

1. 建立 websocket 连接
2. 发送 `hello(role=worker)`
3. 发送 `poll`
4. 收到 `poll-result`
5. 逐条处理事件
6. 使用 websocket 客户端直接发送 `ack`

## 7. CLI 对应关系

- `serve`: 启动中继服务，默认监听 `127.0.0.1:9100`，也可用 `--bind` 覆盖
- `channels`: 查看指定 relay 上当前有哪些 channel 已经有 receiver 连接
- `status`: 通过 `--channel <name>` 向对应 renderer 查询页面状态；必要时用 `--server <WS_URL>` 指定 relay
- `open`: 查询当前 renderer 页面地址并交给系统浏览器打开
- `help`: 向 renderer 查询帮助文档
- `skill`: 向 renderer 查询 skill 文本
- `send`: 发送 `request` 并阻塞等待 `ack`，命令最后一个位置参数直接作为 `payload`
- `poll`: 拉取指定 channel 的队列事件

当前 `help` / `skill` 一类高层命令不应把文档硬编码在 CLI 内部，而是通过协议向指定 channel 上的 renderer 查询。

当前 CLI 的 stdout 都输出协议帧本身的 Cirru EDN 文本，或直接输出 renderer 返回的文本内容，便于脚本继续解析。

## 8. renderer 请求约定

renderer 的 `help` / `skill` / `status` 与布局投递都走命令显式指定的 channel，不再额外保留一个固定 `renderer` 频道。

### 8.1 `help` 请求

CLI 通过 `request(channel=<命令行 --channel>)` 发送：

```cirru
{}
  :op |help
  :topics $ [] |chart |mermaid
```

字段说明：

- `:op` 固定为 `help`
- `:topics` 可选，用于按名称筛选组件说明；为空时返回全部概览

renderer 成功处理后，返回 `ack(ok=true)`，其中 `payload` 为一段 Cirru EDN map，至少包含：

- `:status`
- `:kind`
- `:renderer`
- `:commands`
- `:components`

### 8.2 `skill` 请求

CLI 通过 `request(channel=<命令行 --channel>)` 发送：

```cirru
{}
  :op |skill
```

renderer 成功处理后，返回 `ack(ok=true)`，其中 `payload` 至少包含：

- `:status`
- `:kind`
- `:renderer`
- `:title`
- `:text`

其中 `:text` 是 renderer 当前暴露出来的 skill 内容。

### 8.3 `status` 请求

CLI 通过 `request(channel=<命令行 --channel>)` 发送：

```cirru
{}
  :op |status
```

renderer 成功处理后，返回 `ack(ok=true)`，其中 `payload` 至少包含：

- `:status`
- `:kind`
- `:renderer`
- `:title`
- `:page_url`
- `:commands`
- `:channel`
- `:channels`

`open` 命令可以基于这份返回结果里的 `:page_url` 调用系统浏览器。

## 9. receiver 侧 payload 约定

像 `genui` 这样的 channel 名称只是发送方与 receiver 之间的约定。relay 只负责转发 Cirru EDN `payload`，不在 CLI 或协议层硬编码具体的数据结构。

如果某个 receiver 约定了特定 payload 形状、校验规则或 ack 内容，应由该 receiver 自己的文档定义，例如 `edn-renderer` 的运行时协议说明。

推荐流程仍然是：

1. 用户启动 `serve`
2. 浏览器打开 receiver 页面，并通过 URL 参数如 `?channel=genui` 选中 channel；如果 relay 端口被改过，也可以通过 `?port=<PORT>` 指向同一个端口
3. 发送方把约定好的 Cirru EDN 数据作为 `request(channel=genui)` 的 `payload` 发给 relay
4. relay 把 `event(channel=genui)` 投递给 receiver
5. receiver 按自己的约定处理 payload 并返回 `ack`
