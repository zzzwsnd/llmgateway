# LLMGateway 开发约定

## 项目定位

LLMGateway 是一个基于 FastAPI 的模块化单体，负责统一接入多个下游大模型供应商，并提供直接会话、流式输出、会话管理以及后续 Agent Loop、Tools、MCP、Skills 和 RBAC 能力。

当前阶段优先保证核心会话链路和模型供应商接入清晰，不提前实现尚未确认的扩展能力。

## 架构原则

项目采用轻量 DDD：

- `domain` 负责业务概念、领域规则和可复用的业务能力。
- `application` 负责具体用例、执行流程和跨领域编排。
- `api` 负责 HTTP 协议、路由和响应输出。
- `admin/models` 负责外部协议和管理契约的 Pydantic 模型。
- `infrastructure` 负责 PostgreSQL、HTTP、认证、配置和观测等技术实现。
- 不为了形式引入完整 DDD、微服务或多余的抽象层。

## 目录职责

```text
src/llmgateway/
├── api/                         # FastAPI 路由和依赖
├── application/
│   ├── chat/                    # 直接会话和 Agent Loop 用例编排
│   └── runs/                    # 一次执行的生命周期管理
├── domain/
│   ├── sessions/                # 会话领域
│   ├── messages/                # 消息领域
│   ├── models/                  # 下游模型供应商能力
│   │   └── implementations/     # 各供应商工厂和实现
│   ├── integrations/            # Tools、MCP、Skills，后续设计
│   └── authorization/           # 权限、角色和授权策略
├── infrastructure/              # 数据库及其他技术实现
└── core/                        # 通用基础能力
```

## 模型供应商模块

`domain/models` 只负责下游模型供应商能力，不放 Tools、MCP、Skills 或会话逻辑。

新增模型供应商时：

1. 在 `domain/models/implementations/` 新增供应商实现文件。
2. 实现统一的模型产品接口和工厂接口。
3. 在模型工厂入口增加供应商选择映射。

应用层只能依赖统一模型接口，不应直接依赖某个供应商 SDK 或供应商响应类型。

## 外部协议模型

`admin/models` 使用 Pydantic 定义：

- OpenAI Chat Completions 请求和响应
- OpenAI Responses 请求和响应
- Anthropic Messages 请求和响应
- 会话、消息、工具、事件、错误和分页模型

外部协议模型不能直接贯穿领域层。进入应用层后，应转换为内部统一请求、结果和事件类型。

## 会话和执行

- `Session` 表示长期会话。
- `Message` 表示会话中的语义消息。
- `Run` 表示一次应用执行过程，放在 `application/runs`。

一次请求通常遵循：

```text
接收请求
→ 加载或创建 Session
→ 保存用户 Message
→ 创建 Run
→ 构建上下文
→ 调用模型或 Agent Loop
→ 返回 JSON 或 SSE
→ 保存最终 assistant Message
→ 更新 Run 状态和 usage
```

流式输出第一阶段只向客户端发送增量事件，在执行完成后保存完整 assistant 消息；不逐 token 写 PostgreSQL，除非后续明确需要断线恢复或事件回放。

## 数据和基础设施

- PostgreSQL 作为主数据库。
- SQLAlchemy 负责数据库映射和查询。
- asyncpg 作为异步驱动。
- Alembic 负责迁移。
- Pydantic 负责请求、响应、配置和内部契约校验，不替代 ORM。

数据库实现放在 `infrastructure/database`，不要把 SQLAlchemy 模型直接当作领域实体使用。

## 当前暂缓内容

以下模块先保留目录和占位文件，不提前扩展设计：

- `domain/integrations/tools`
- `domain/integrations/mcp`
- `domain/integrations/skills`
- RBAC 的完整租户、角色继承和策略组合
- Agent Loop 的复杂恢复、暂停和分布式执行
- 流式事件持久化和断线续传

## 开发约束

- 新增功能先讨论边界，再实现代码。
- 优先修改现有职责明确的模块，不随意新增顶层目录。
- 不把供应商 SDK 类型泄漏到 API 或领域外部。
- 不在 FastAPI 路由中实现模型调用、会话持久化或 Agent Loop。
- 同步和流式接口应复用同一套应用执行逻辑。
- 测试目录保持精简，按实际功能增长，不提前创建大量空目录。
