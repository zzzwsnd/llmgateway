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

## 模型重试、fallback 与 JSON 重试

重试次数和退避时间由 YAML 顶层 `retry` 控制；供应商 SDK 自带重试必须关闭。
`max_retries_per_model` 是每个模型在首次调用之外允许的重试次数，单模型最多调用
`1 + max_retries_per_model` 次。第 `n` 次重试前等待
`initial_delay_seconds * backoff_multiplier^n` 秒，`n` 从 0 开始。`attempts` 统计实际发起的
全部上游调用，包括供应商故障重试、JSON 纠错重试和 fallback 调用。同一模型内的故障与
JSON 重试共用预算，切换到下一个 fallback 模型时重置单模型预算。

非流式调用仅对可重试的上游故障（连接失败、超时、限流和 Responses 的 `failed` /
`incomplete` 终态）在当前模型重试；重试耗尽后按 YAML 的 `fallback` 链尝试下一模型。
非可重试的普通上游异常直接进入下一 fallback，不在当前模型重试。请求及主模型校验错误、
供应商返回的网关业务错误直接返回，不得借切换模型掩盖。所有候选模型失败时返回
`model_unavailable`。fallback 可以多级配置，但每一级都必须与原模型保持相同的上游协议，
并通过模型能力校验。

流式调用只能在首个非空 delta 产生前因可重试错误切换 fallback。已输出 delta 后，仅当
供应商给出 `resume_token` 时才允许在原模型上重试；游标不得跨模型复用。已输出 delta 后
无法恢复或重试预算耗尽时返回 `upstream_stream_failed`，不得切换 fallback 并拼接模型文本。
非可重试的流式异常直接结束生成，不重试也不降级。

JSON 纠错仅适用于带 `response_schema` 的非流式结构化输出。调用供应商前必须校验 Schema，
无效时返回 `invalid_response_schema`。解析失败时先尝试仅补齐末尾缺失的 `}` / `]`，补齐后
通过 JSON 解析及 Schema 校验即可直接返回，不消耗额外调用。无法修复时生成 `invalid_json`
诊断；能解析但不符合 Schema 时生成包含缺失/错误字段路径的 `schema_validation_failed`
诊断。尚有单模型重试预算时，使用原始消息、非空的失败 assistant 输出和
`json_parsing.retry_prompt.active_version` 对应模板生成的纠错 user 消息重试同一模型；纠错
请求仍须携带原始结构化输出要求。预算耗尽后返回对应的 JSON 错误，不进入 fallback。
成功返回到网关的各次上游 completion 用量必须累计到最终成功响应中。

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

PostgreSQL 的 `calls` 是调用及会话状态的事实来源，并保存会话活跃时间；`call_attempts`
保存每次真实上游模型请求。不得保存 CAS、generation 或 execution fencing 字段；状态转换
依靠数据库事务、事务内行锁和合法状态校验。不得创建独立 stream 会话表、调用事件表或结果
正文表。出现分区时应以持久化状态为基准。Redis 只保存会话级 Stream
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
游标并采用 at-least-once 语义。Redis 可达但终态 Stream 已过期时返回
`410 stream_expired`；客户端使用新的幂等键重新提交，并通过 `retry_of_call_id` 关联旧调用。
Redis 不可用必须返回 503，不得误判为过期或触发模型重试。

第一版不在生成进程宕机后迁移或重新执行供应商请求。活跃和排队任务周期性刷新
PostgreSQL 调用活跃时间；校准必须在数据库事务内锁定调用记录并重新检查过期状态，再把
孤儿调用收敛为失败，不得重复调用模型。Redis 短时不可用不能取消仍在运行的供应商流。任何请求正文、
响应正文、Prompt 正文或 JSON Schema 都不得写入 `calls` 或 `call_attempts`。
PostgreSQL 和 Redis 的 host、port、database、user、password 只能通过 YAML 中配置的
环境变量名延迟读取，不得在 YAML 中提交实际连接参数或连接串。

## 调用审计

审计只允许两个业务维度：`calls` 表示一次客户端逻辑调用，`call_attempts` 表示该调用下每次
真实上游请求。attempt 类型固定为 `initial`、`retry`、`fallback`、`json_retry`。本地只补齐
JSON 尾部括号不新增 attempt；参数缺失、参数错误或 JSON 无法修复而重新请求模型时新增
`json_retry`。Token、成本、实际模型和尝试次数必须从 `call_attempts` 聚合，不得在 `calls`
维护重复计数。`GET /v1/traces` 必须读取 PostgreSQL 持久化审计数据。

## 提示词编写规范

仓库中的系统提示词、纠错提示词和任务提示词必须采用稳定、可审计的结构。新增或修改提示词时
遵循以下规则：

1. `[权重:100]` 为不可违反的硬约束，`[权重:90]` 为核心任务要求，`[权重:70]` 为质量优化。
   每条要求必须单独标注权重；要求冲突时执行权重更高的要求，同权重冲突时执行排列在前的要求。
2. `[权重:100]` 提示词必须定义固定身份，并明确该身份的职责范围，不得只写“你是一个助手”。
3. `[权重:100]` 提示词必须分别定义角色、目标、边界和输出风格；关键输出必须具有可以由程序、
   Schema、测试或人工检查清单验证的通过条件。
4. `[权重:100]` 禁止使用“效果好”“尽量准确”“高质量”“合理”等无法独立判定是否达成的描述。
   应改写为可验证条件，例如“输出必须能被标准 JSON 解析器解析”或“输出必须通过给定 JSON
   Schema 的全部约束”。
5. `[权重:90]` 复杂任务必须拆成按顺序执行的步骤。每一步只描述一个主要动作，并声明该步骤的
   可检查产物或完成条件。
6. `[权重:90]` 约束必须说明允许行为、禁止行为和失败条件；不得依赖未声明的隐含规则。
7. `[权重:70]` 在示例能够降低格式或边界歧义时，提供一至两个正例和一至两个反例，并说明示例
   只用于展示规则，不得覆盖当前任务输入或约束。
8. `[权重:70]` 提示词末尾应包含静默自检清单。自检只影响最终结果，不得要求模型输出思考过程、
   推理链或额外解释。

推荐结构固定为：`身份 -> 目标 -> 输入诊断 -> 边界与约束 -> 操作步骤 -> 输出契约 ->
Few-shot 正反例 -> 静默自检`。提示词使用模板占位符时，配置模型必须校验必需占位符和未知
占位符；示例中的字面量花括号必须按模板语法转义。

## 常用命令

```powershell
$env:UV_CACHE_DIR = "$PWD\.uv-cache"
uv sync --dev --locked
uv run --locked pytest -q
uv run --locked python -m compileall -q app gateway.py tests
$env:GATEWAY_SNOWFLAKE_WORKER_ID = "0" # 仅用于单进程开发；部署须按进程分配唯一 ID。
uv run --locked uvicorn gateway:app --env-file .env --loop asyncio:SelectorEventLoop --host 0.0.0.0 --port 8000
```
