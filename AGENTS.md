# LLM Gateway 维护指南

## 分层规则

依赖方向必须严格遵循：

```text
controller -> service -> dao -> mapper
```

所有层都可以依赖 `app.model` 和不包含业务逻辑的 `app.core` 工具。`app/main.py`
是唯一允许跨层构造对象的组合根。不得跳过中间层、反向导入，或通过间接导入伪造其他
分层边界。

- `controller`：只负责 HTTP 路由声明、Pydantic 请求/响应边界和流式响应包装。不得包含
  业务编排或数据访问。
- `service`：负责业务编排、重试、降级、调用追踪、协议校验、响应呈现和工厂管理。
  不得导入供应商 SDK。
- `dao`：对数据调用和协议调用进行薄封装。不得包含路由、降级或响应格式化逻辑。
- `mapper`：负责实际的 SDK 和存储访问、SDK 异常转换及供应商响应提取。
- `model`：包含所有请求、响应、DTO、实体、枚举和配置数据模型。
- `core`：包含可执行的配置工具、全局错误及处理器、日志和无状态工具。
  `app/core/config.py` 只负责路径解析、YAML 加载/校验/缓存和延迟读取密钥；不得用于
  存放应用、模型、重试或供应商常量。

架构测试必须持续递归扫描每一层下的所有 Python 文件，包括嵌套的供应商实现模块。

## 配置与密钥

配置文件按以下顺序解析：

1. 已设置时使用 `GATEWAY_CONFIG_FILE` 指向的文件。
2. 存在时使用 `config/gateway.yaml`。
3. 最后使用已提交的开发配置 `config/gateway.example.yaml`。

本地 `config/gateway.yaml` 不得提交到 Git。YAML 中只能保存 `api_key_env` 环境变量名，
不得保存原始 API Key。供应商凭据必须在首次调用该供应商时延迟读取。不得在日志、响应、
模型发现接口或其他公开元数据中暴露 API Key、base URL、供应商模型名、价格或降级映射。

## 扩展模型、供应商和协议

公开的模型、供应商和协议标识只能使用枚举：`ModelEnum`、`ModelProviderEnum` 和
`LLMProtocolEnum`。请求字段和 YAML 映射键必须能够解析为这些枚举，不支持在运行时使用
任意字符串创建新的模型名或供应商名。

新增模型时，需要添加对应的 `ModelEnum` 成员，并在 YAML 中增加以该枚举值为键的模型
路由配置。

新增供应商时，需要完成以下步骤：

1. 添加 `ModelProviderEnum` 成员。
2. 添加对应的 YAML 供应商配置。
3. 在 `app/service/provider/` 下实现 `ModelProvider`。
4. 在 `app/main.py` 传给 `ProviderFactory` 的
   `dict[ModelProviderEnum, ModelProvider]` 中完成注册。

新增协议时，需要完成以下步骤：

1. 添加 `LLMProtocolEnum` 成员。
2. 实现独立的 mapper 和薄 `ProviderDao`。
3. 在传给 `ProtocolFactory` 的 `dict[LLMProtocolEnum, ProviderDao]` 中注册该 DAO。

供应商选择与协议选择必须保持为两个相互独立的工厂注册表。

禁止协议桥接。`POST /v1/chat/completions` 只能调用 `CHAT_COMPLETIONS` 上游模型，
`POST /v1/responses` 只能调用 `RESPONSES` 上游模型。协议不匹配时，必须在调用任何供应商
之前返回 `protocol_mismatch`。降级模型必须与原模型使用相同的上游协议。旧版 `/v1/llm`
接口保持协议中立，使用所选模型配置的上游协议。

## 支持范围

当前原生协议支持：文本输入输出、JSON 结构化输出、非流式调用和流式调用。

当前不支持：工具或函数调用、图像/音频等多模态输入输出、后台 Responses 任务、
通用对话上下文会话以及跨协议转换。可恢复 SSE 生成会话见下文；流式调用不能与
结构化输出同时使用。

公开接口如下：

- `POST /v1/llm`
- `POST /v1/llm/stream`
- `POST /v1/chat/completions`
- `POST /v1/responses`
- `GET /v1/models`
- `GET /v1/traces`
- `POST /v1/stream-sessions`
- `GET /v1/stream-sessions/{session_id}`
- `GET /v1/stream-sessions/{session_id}/events`
- `DELETE /v1/stream-sessions/{session_id}/connections/{connection_id}`
- `POST /v1/stream-sessions/{session_id}/cancel`

## 可恢复 SSE 会话

可靠流式调用必须区分模型生成会话、客户端物理 SSE 连接和供应商物理连接。客户端退出、
网络断开只能关闭对应的本地订阅连接；单连接 `detach` 通过会话 Stream 中的
`control.detach` 关闭匹配的 `connection_id`，不得取消模型生成任务。连接标识仅保存在
订阅迭代器内存中，不持久化独立连接记录。
`cancel` 是独立的业务命令，负责停止供应商流并调用补偿服务；当前补偿实现为空操作，后续
循环业务接入时应替换为基于持久化 outbox 的实现。

PostgreSQL 是会话状态和最终完整文本的事实来源，同时保存 generation/version fencing
和会话活跃时间。出现分区时应以持久化状态为基准。Redis 只保存会话级 Stream
`gateway:session:{session_id}:events`，其中包含增量、终态和 `control.cancel` /
`control.detach` 控制事件。不得保存节点、IP、Producer 租约、心跳、连接位置或独立取消
标记，不得根据 IP 定位生成服务。内部控制事件不得输出为公开 SSE 事件。

会话 ID 必须为 `session-{雪花Id}`，使用 `app/core/snowflake.py` 中的线程安全生成器，
由 `app/main.py` 注入。通过 `sessions.snowflake_worker_id_env` 指定环境变量名，默认
`GATEWAY_SNOWFLAKE_WORKER_ID`；每个并发生成进程必须分配唯一的 0..1023 worker ID，
包括同一主机上的多个进程。worker ID 只保证 ID 唯一性，不用于服务定位。

Producer 必须由独立于 `StreamingResponse` 的后台任务持有。一个会话允许多个独立 SSE
连接。生成服务与 SSE 订阅服务通过会话 Stream 解耦，不需要部署在同一进程或服务；
当前创建接口仍由本地 supervisor 接收生成任务，不增加独立任务队列。每个订阅者使用
独立 `XREAD` 游标，禁止用 consumer group 分摊事件。重连以客户端 `Last-Event-ID` 为
游标并采用 at-least-once 语义。Stream 过期后使用
PostgreSQL 完整结果快照兜底，不要求重现原始 chunk 边界。

第一版不在生成进程宕机后迁移或重新执行供应商请求。活跃和排队任务周期性刷新
PostgreSQL 会话活跃时间；校准只根据过期状态和 CAS 把孤儿会话收敛为失败，不得
重复调用模型。Redis 短时不可用不能取消仍在运行的供应商流；最终结果仍应写入数据库。
数据库和 Redis 连接串只能通过 YAML 中配置的环境变量名延迟读取，不得提交连接串值。

## 常用命令

```powershell
$env:UV_CACHE_DIR = "$PWD\.uv-cache"
uv sync --dev --locked
uv run --locked pytest -q
uv run --locked python -m compileall -q app gateway.py tests
$env:GATEWAY_SNOWFLAKE_WORKER_ID = "0" # 仅用于单进程开发；部署须按进程分配唯一 ID。
uv run --locked uvicorn gateway:app --host 0.0.0.0 --port 8000
```
