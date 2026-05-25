# Cirru EDN Relay 协议

本文档定义 `cirru_edn_relay` 使用的 websocket 文本协议。所有消息都必须使用 Cirru EDN 编码，且 websocket 只发送文本帧，不发送二进制帧。

## 1. 总体约定

- websocket 地址示例: `ws://127.0.0.1:9001`
- 每一帧都是一个 Cirru EDN map，对应 Rust 侧的 `WireMessage`
- 顶层字段名固定，采用 snake_case
- `payload` 字段直接承载 Cirru EDN 数据，不再把数据额外包一层字符串
- `channel` 用于路由消息，同一个 channel 可以有多个浏览器或 worker 订阅
- `id` 是一次请求-回执往返的唯一标识，建议由发送方生成 UUID
- 服务端收到 `request` 后，若当前没有在线订阅者，则进入队列；后续可被订阅连接自动取到，或者由 `poll` 主动拉取

## 2. 连接初始化

任何客户端连上 websocket 后，第一条业务消息都应该先发 `hello`。

### hello

字段说明:

- `kind`: 固定为 `"hello"`
- `role`: `"browser"`、`"cli"`、`"worker"` 三选一
- `client_id`: 可选，客户端自定义标识；不传时由服务端回填 session id
- `channels`: 可选数组，表示当前连接要订阅的频道列表

示例:

```cirru
{}
  :kind |hello
  :role |browser
  :client_id |page-main
  :channels $ [] |demo
```

### hello-ok

服务端确认握手成功后返回:

- `kind`: 固定为 `"hello-ok"`
- `client_id`: 服务端最终确认的连接标识

示例:

```cirru
{}
  :kind |hello-ok
  :client_id |page-main
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
- 同一个 `id` 只接受第一条有效回执，后续重复回执会得到错误

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
- 因此 `poll` 适合接 worker 进程，worker 拉取后应自行调用 `reply` 或直接实现 websocket 回执

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
2. 发送 `hello(role=browser, channels=[...])`
3. 等待 `hello-ok`
4. 持续接收 `event`
5. 处理完成后发送 `ack`

### 6.2 命令行发送并等待回执

1. 建立 websocket 连接
2. 发送 `hello(role=cli)`
3. 发送 `request`
4. 收到 `accepted`
5. 等待服务端转发回来的 `ack`

### 6.3 worker 拉取队列任务

1. 建立 websocket 连接
2. 发送 `hello(role=worker)`
3. 发送 `poll`
4. 收到 `poll-result`
5. 逐条处理事件
6. 使用 `reply` 或自定义 websocket 客户端发 `ack`

## 7. CLI 对应关系

- `serve`: 启动中继服务
- `genui`: 向固定的 `genui` channel 发送经过校验的布局描述，并等待浏览器确认已经存入 store 且可渲染
- `send`: 发送 `request` 并阻塞等待 `ack`
- `poll`: 拉取指定 channel 的队列事件
- `reply`: 发送 `ack`

当前 CLI 的 stdout 都输出协议帧本身的 Cirru EDN 文本，便于脚本继续解析。

## 8. `genui` 频道约定

`genui` 是给 `edn-renderer` 这类前端渲染器使用的保留频道。

### 8.1 请求载荷

`genui` 的 `payload` 不是任意业务对象，而是一棵布局树。当前约定节点字段如下:

- `:type` 必填，字符串，支持 `"column"`、`"row"`、`"card"`、`"text"`、`"badge"`、`"divider"`、`"markdown"`、`"mermaid"`、`"chart"`、`"button"`、`"input"`
- `:children` 可选，列表，仅容器节点使用
- `:text` 可选，给 `text`、`button`、`card` 标题、`markdown`、`mermaid` 使用
- `:placeholder` 可选，给 `input` 使用
- `:name` 可选，给 `input` 使用
- `:series` 可选，给 `chart` 使用，元素为 `{:label string :value number}`

示例:

```cirru
{}
  :type |card
  :text "|Demo Card"
  :children $ []
    {} (:type |badge) (:text |preview)
    {} (:type |divider)
    {} (:type |text) (:text "|Hello from genui")
    {} (:type |row)
      :children $ []
        {} (:type |button) (:text |Confirm)
        {} (:type |input) (:name |email) (:placeholder |Email)
```

### 8.2 本地校验

`genui` 命令在发消息之前会先做一轮本地校验:

- payload 必须是合法的 Cirru EDN
- 根节点必须能反序列化为布局节点
- 不支持的 `:type` 会直接报错
- `text`/`badge`/`button`/`markdown`/`mermaid` 节点要求非空 `:text`
- `chart` 节点要求非空 `:series`，并且每个元素都要有非空 `:label` 与有限数值 `:value`
- `input` 节点至少要有 `:name` 或 `:placeholder`

如果本地校验失败，命令不会连 websocket，也不会把错误 layout 发给浏览器。

### 8.3 浏览器回执

浏览器成功接收并应用 layout 后，会返回 `ack(ok=true)`，其中 `payload` 也是一段 Cirru EDN 数据，格式如下:

```cirru
{}
  :status |ok
  :layout_id |layout-<request-id>
```

浏览器侧如果拒绝渲染，应返回 `ack(ok=false)` 并在 `error` 字段中放入可读错误文本。

### 8.4 推荐流程

1. 用户启动 `serve`
2. 浏览器打开 `edn-renderer`，自动连接 relay，并订阅 `genui`
3. 命令行执行 `genui <LAYOUT>`
4. relay 把 `event(channel=genui)` 投递给浏览器
5. 浏览器把 layout 写入自己的 store 并渲染
6. 浏览器回 `ack`，CLI 打印 `genui ok <layout-id>`
