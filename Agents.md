# Agents 维护约定

本文件用于约束后续对 `cirru_edn_relay` 的开发方式，避免协议、实现和文档分叉。

## 1. 目标边界

- 这是一个单二进制命令行工具
- 核心职责只有三类: 中继 websocket 消息、用 Cirru EDN 序列化协议、提供简单的命令行入口
- 不要把业务逻辑写进 relay；relay 只负责路由、排队、回执关联和协议校验

## 2. 协议变更规则

- 修改协议时，必须同步更新 `PROTOCOL.md`
- 新增字段优先走向后兼容，避免直接重命名或删除既有字段
- 如果未来必须做破坏性变更，先引入显式版本字段，再迁移 CLI 和浏览器端
- `payload` 当前约定为直接的 Cirru EDN 数据；协议变更时必须同步更新 `PROTOCOL.md`

## 3. 代码约定

- 继续使用 `cirru_edn` 做协议帧编码和解码
- websocket 仅发送文本帧
- `id` 必须由请求侧生成，并用于一条请求对应一条回执
- 服务端应维持最小状态: 连接表、订阅表、待回执表、离线队列表
- 修问题优先找根因，不要在命令层堆绕路逻辑

## 4. 实现策略

- 优先保持依赖简单，避免为了小功能引入完整 web 框架
- 若需要扩展浏览器接入，优先复用现有 `hello/request/event/ack/poll` 协议，不先拆新协议
- relay 的本地持久化状态统一放在 `~/.config/edn-relay.cirru`
- 面向用户的命令尽量默认读取当前 relay 状态，不要要求每次重复填写服务地址和端口
- `help` / `skill` 这类高层命令应通过协议查询当前 renderer，不要把文档硬编码在 relay CLI 里
- 如果增加测试，优先补 websocket 集成测试，覆盖 `send -> event -> ack`、`request -> queue -> poll -> reply` 和 `CLI -> renderer docs/skill query`

## 5. 交付检查

每次改动完成后至少做下面一项:

- `cargo check`
- 必要时补一个最小 smoke test，覆盖 websocket 中继链路

如果变更涉及协议字段、CLI 行为或输出格式，还应额外核对:

- `PROTOCOL.md` 是否同步更新
- CLI 示例是否仍然成立
- 浏览器侧是否仍能按文本帧解析
