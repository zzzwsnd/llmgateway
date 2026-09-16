# LLM Gateway 轻量级 MVC 拆分设计

## 目标

将当前 `gateway.py` 单文件应用拆分为严格的 `controller/service/dao/mapper/model/core` 六层结构，同时保持现有 HTTP 协议、运行行为和 `uvicorn gateway:app` 启动方式兼容。

本轮不接入 PostgreSQL 或 Redis，不新增虚假的数据库客户端。Prompt、调用追踪和模型配置继续使用内存或静态配置实现，并通过 mapper 接口隔离，为后续替换成 PG/Redis 实现保留边界。

## 当前功能

现有网关提供以下业务功能：

1. `POST /v1/llm`：非流式 LLM 调用。
2. `POST /v1/llm/stream`：SSE 流式 LLM 调用。
3. `GET /v1/traces`：查询当前进程内的调用追踪记录。
4. 统一请求模型：消息、模型别名、超时、Prompt 版本、结构化输出 Schema。
5. OpenAI Compatible 上游协议适配，目前配置为 DeepSeek 主备模型。
6. 主模型临时故障重试，并降级到备用模型。
7. 受控 Prompt 模板选择、变量替换和版本标识。
8. JSON 结构化输出请求、解析和 JSON Schema 校验。
9. Token 用量、调用成本、延迟、尝试次数、成功或失败状态追踪。
10. 请求字段约束、模型白名单、能力组合校验和稳定业务错误码。

## 当前工程能力

现有代码已经具备：

- FastAPI 异步接口和 Pydantic 输入输出契约。
- OpenAI Compatible SDK 的异步非流式与流式调用。
- 供应商密钥仅从环境变量读取。
- 主备模型降级和有限重试。
- SSE 标准事件编码。
- JSON Schema 结构化结果验证。
- Prompt 模板集中管理及版本选择。
- 基础成本核算和结构化调用日志。
- 通过 `Provider` Protocol 对上游调用进行了一定程度的抽象。

当前工程缺口：

- 所有职责集中在一个文件，层次间没有明确依赖约束。
- 没有自动化测试，重构缺少回归保护。
- `pyproject.toml` 未声明实际运行和测试依赖。
- Trace、Prompt 和配置均为进程内数据，重启后丢失且不适合多实例共享。
- Controller 手工捕获业务异常，没有全局异常处理。
- 全局 provider 和存储对象不利于依赖替换和单元测试。
- 没有健康检查、鉴权、限流、指标、分布式追踪或持久化能力。

## 方案选择

采用顶层技术分层：

```text
gateway.py
app/
  main.py
  controller/
  service/
  dao/
  mapper/
  model/
  core/
tests/
docs/
```

依赖方向固定为：

```text
controller -> service -> dao -> mapper
                    \-> model
controller/service/dao/mapper -> model
所有层 -> core（仅使用全局基础能力）
```

禁止反向依赖。`model` 不依赖其他业务层；`core` 不依赖 controller、service、dao 或 mapper。

## 分层职责

### controller

只定义 HTTP 接口：路由、请求模型、响应模型、HTTP 返回载体。Controller 调用 Service，不执行业务校验、重试、降级、Prompt 拼装、数据读写或异常转换。

- `llm_controller.py`：`/v1/llm` 与 `/v1/llm/stream`。
- `trace_controller.py`：`/v1/traces`。

### service

只负责业务逻辑编排：请求组合校验、Prompt 注入、模型选择、重试、降级、结构化结果解析、调用追踪编排和流式生命周期。

- `llm_service.py`：非流式和流式调用主流程。
- `prompt_service.py`：Prompt 渲染和消息构建。
- `trace_service.py`：追踪实体构建、成本计算和保存编排。

Service 不直接访问字典、列表、环境变量、数据库、Redis 或供应商 SDK。

### dao

对数据操作进行简单封装，向 Service 提供稳定、面向业务的读写方法，不承载流程编排。

- `model_dao.py`：按平台模型名取得模型配置和价格。
- `prompt_dao.py`：按名称与版本取得 Prompt 模板。
- `trace_dao.py`：保存和列出调用追踪。
- `provider_dao.py`：封装统一供应商调用接口。

DAO 只调用 Mapper，不导入 FastAPI。

### mapper

执行真实的数据获取或外部系统交互。本轮提供内存与 OpenAI Compatible 实现：

- `memory_model_mapper.py`：模型配置与价格静态数据。
- `memory_prompt_mapper.py`：内存 Prompt 模板。
- `memory_trace_mapper.py`：进程内 Trace 列表。
- `openai_mapper.py`：OpenAI Compatible SDK 调用和协议转换。

未来新增 `postgres_*_mapper.py` 或 `redis_*_mapper.py`，替换应用装配中的实现即可；不得因此修改 Controller。

### model

包含所有请求、响应、实体和 DTO，不包含数据访问或流程编排：

- `request.py`：`Message`、`PromptSelection`、`LLMRequest`。
- `response.py`：`Usage`、`LLMResponse`、标准错误响应。
- `entity.py`：`PromptTemplate`、`CallTrace`、`ModelConfig`、价格实体。
- `dto.py`：Mapper/DAO 与 Service 之间使用的供应商调用结果等传输对象。

### core

只放全局能力：

- `config.py`：应用元数据、环境变量名和全局静态配置。
- `errors.py`：`GatewayError` 及稳定错误码。
- `exception_handlers.py`：全局业务异常和兜底异常处理器。
- `logging.py`：日志实例和初始化。
- `utils.py`：无状态通用工具，例如 SSE 编码和可重试异常判断。
业务规则不得为了复用而放入 `core`。

## 应用装配与兼容性

`app/main.py` 是唯一的组合入口：创建默认 Mapper、DAO 和 Service 对象，创建 FastAPI 实例，注册全局异常处理器，并把 Service 注入 Controller 的路由工厂。默认使用内存 Mapper 和 OpenAI Mapper。除这个组合入口外，任何模块都不得跨层装配依赖。

根目录 `gateway.py` 缩减为兼容入口：

```python
from app.main import app

__all__ = ["app"]
```

因此原命令继续有效：

```shell
uvicorn gateway:app
```

HTTP 路径、请求字段、响应字段、SSE 事件名称、错误码、主备策略和默认配置保持不变。

## 请求数据流

非流式请求：

```text
HTTP -> LLM Controller -> LLM Service
  -> Prompt/Model DAO -> Memory Mapper
  -> Provider DAO -> OpenAI Mapper -> 上游模型
  -> Trace Service -> Trace DAO -> Memory Trace Mapper
  -> LLM Response -> HTTP
```

流式请求遵守现有语义：首个内容块发出前允许切换备用模型；首块发出后不得重放内容，只在 SSE 流中返回失败事件。

## 错误处理

`GatewayError` 保留 `code`、`message` 和 `status_code`。Controller 不再 `try/except` 转换业务异常；`core/exception_handlers.py` 统一转换为现有 FastAPI `detail` 结构：

```json
{
  "detail": {
    "code": "unknown_model",
    "message": "模型不在 Gateway 允许列表中"
  }
}
```

Pydantic/FastAPI 的请求校验响应保持框架默认行为。流已经开始后的上游错误继续编码为 `response.failed` SSE 事件，不能改成 HTTP 状态码。

## 测试策略

重构前先增加行为回归测试并确认能够暴露缺失的新模块，然后逐层迁移：

- Model：字段约束及 `stream + response_schema` 禁止组合。
- Prompt Service：正常渲染、未知模板和缺少变量。
- LLM Service：模型校验、成功调用、临时失败重试、主备降级、非法 JSON、Schema 不匹配。
- 流式 Service：正常事件、首块前降级、首块后失败。
- Trace Service/DAO：成本、状态及列表读写。
- Controller：三个现有接口、全局错误转换和兼容入口。

测试使用可替换的 Mapper/DAO 实例，不进行真实网络调用，也不依赖 PG/Redis。

## PG/Redis 后续占位

本轮仅记录扩展约定，不创建无法运行的数据库类：

- PostgreSQL 建议持久化 Prompt 版本、模型配置、价格和历史 Trace。
- Redis 建议承载配置缓存、Prompt 缓存、短期 Trace 缓冲、限流计数或分布式锁。
- 新实现必须遵守现有 Mapper 方法契约。
- 数据库连接、连接池、迁移脚本、序列化策略和缓存一致性在实际接入时单独设计。

## 非目标

本轮不改变 API，不新增管理接口，不接入真实 PG/Redis，不加入鉴权、限流、健康检查、指标平台或分布式追踪，也不调整现有模型价格和 Prompt 内容。

## 完成标准

- `gateway.py` 只保留兼容导出。
- 所有代码按六层职责归位，无反向依赖或跨层直连。
- 三个现有接口及其协议保持兼容。
- 自动化测试覆盖关键正常、失败、重试、降级和流式路径。
- 项目依赖和运行方式有明确文档。
- PG/Redis 后续接入点在文档中明确，但当前运行不依赖它们。
