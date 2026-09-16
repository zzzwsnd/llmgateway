# LLM Gateway Lightweight MVC Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将单文件 LLM Gateway 重构为严格的 `controller/service/dao/mapper/model/core` 六层工程，同时保持现有 API 和 `uvicorn gateway:app` 兼容。

**Architecture:** Controller 仅暴露 FastAPI 路由，Service 编排模型调用、Prompt、重试、降级和 Trace，DAO 封装数据操作，Mapper 执行内存或 OpenAI Compatible 的真实访问。Model 保存全部请求、响应、实体与 DTO，Core 提供配置、错误、异常处理、日志、工具和依赖装配。

**Tech Stack:** Python 3.14、FastAPI、Pydantic 2、OpenAI Python SDK、jsonschema、pytest、pytest-asyncio、HTTPX、uv。

**Spec:** `docs/superpowers/specs/2026-09-16-lightweight-mvc-refactor-design.md`

## Global Constraints

- 顶层业务代码必须严格划分为 `controller/service/dao/mapper/model/core`。
- 依赖方向必须是 `controller -> service -> dao -> mapper`；各层可依赖 `model` 和无业务依赖的 `core`，禁止反向依赖。
- Controller 只定义 HTTP 接口，不执行业务校验、重试、降级、Prompt 拼装、数据读写或异常转换。
- Service 不直接访问字典、列表、环境变量、数据库、Redis 或供应商 SDK。
- 本轮不接入 PostgreSQL 或 Redis，也不创建不可运行的数据库客户端占位类。
- `POST /v1/llm`、`POST /v1/llm/stream`、`GET /v1/traces` 的协议和语义保持兼容。
- 原启动方式 `uvicorn gateway:app` 必须继续有效。
- 工作目录当前不是 Git 仓库，因此每个任务使用 diff 与测试作为检查点，不执行提交。

---

## File Map

```text
gateway.py                                # 兼容入口，只导出 app
pyproject.toml                            # 运行与测试依赖、pytest 配置
app/__init__.py                           # 应用包
app/main.py                               # FastAPI 创建、Router 和异常处理注册
app/controller/__init__.py
app/controller/llm_controller.py          # /v1/llm 与 /v1/llm/stream
app/controller/trace_controller.py        # /v1/traces
app/service/__init__.py
app/service/llm_service.py                # 调用、重试、降级、结构化输出和流编排
app/service/prompt_service.py             # Prompt 渲染和消息构建
app/service/trace_service.py              # 成本和 Trace 构建/保存
app/dao/__init__.py
app/dao/model_dao.py                      # 模型配置与价格访问封装
app/dao/prompt_dao.py                     # Prompt 访问封装
app/dao/provider_dao.py                   # 上游供应商调用封装
app/dao/trace_dao.py                      # Trace 读写封装
app/mapper/__init__.py
app/mapper/memory_model_mapper.py         # 静态模型配置与价格
app/mapper/memory_prompt_mapper.py        # 内存 Prompt 模板
app/mapper/memory_trace_mapper.py         # 进程内 Trace 存储
app/mapper/openai_mapper.py               # OpenAI Compatible SDK 访问
app/model/__init__.py
app/model/request.py                      # Message、PromptSelection、LLMRequest
app/model/response.py                     # Usage、LLMResponse、错误响应模型
app/model/entity.py                       # ModelConfig、ModelPrice、PromptTemplate、CallTrace
app/model/dto.py                          # ProviderCompletion
app/core/__init__.py
app/core/config.py                        # 应用及重试配置
app/core/errors.py                        # GatewayError、RetryableProviderError
app/core/exception_handlers.py            # 全局业务异常处理
app/core/logging.py                       # logger
app/core/utils.py                         # SSE 编码
tests/test_models.py
tests/test_data_layers.py
tests/test_prompt_and_trace_services.py
tests/test_llm_service.py
tests/test_api.py
tests/test_architecture.py
README.md                                 # 运行、功能、能力和持久化路线
```

## Shared Interfaces

后续任务必须使用以下精确接口，避免跨任务命名漂移：

```python
# app/model/dto.py
@dataclass(frozen=True)
class ProviderCompletion:
    content: str
    usage: Usage

# app/mapper/openai_mapper.py
class OpenAIMapper:
    async def complete(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
        response_schema: dict[str, Any] | None,
    ) -> ProviderCompletion:
        raise NotImplementedError

    async def stream(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
    ) -> AsyncIterator[str]:
        raise NotImplementedError

# app/dao/*.py
class ModelDao:
    def get_config(self, model: str) -> ModelConfig | None:
        raise NotImplementedError

    def get_price(self, model: str) -> ModelPrice | None:
        raise NotImplementedError

class PromptDao:
    def get(self, name: str, version: str) -> PromptTemplate | None:
        raise NotImplementedError

class TraceDao:
    def save(self, trace: CallTrace) -> None:
        raise NotImplementedError

    def list_all(self) -> list[CallTrace]:
        raise NotImplementedError

class ProviderDao:
    async def complete(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
        response_schema: dict[str, Any] | None,
    ) -> ProviderCompletion:
        raise NotImplementedError

    def stream(
        self,
        config: ModelConfig,
        messages: list[Message],
        timeout_seconds: float,
    ) -> AsyncIterator[str]:
        raise NotImplementedError

# app/service/*.py
class PromptService:
    def render(self, selection: PromptSelection) -> Message:
        raise NotImplementedError

    def build_messages(self, request: LLMRequest) -> list[Message]:
        raise NotImplementedError

class TraceService:
    def record(
        self,
        request_id: str,
        requested_model: str,
        actual_model: str | None,
        prompt: PromptSelection | None,
        usage: Usage,
        latency_ms: int,
        attempts: int,
        status: Literal["success", "failed"],
        error_code: str | None = None,
    ) -> CallTrace:
        raise NotImplementedError

    def list_traces(self) -> list[CallTrace]:
        raise NotImplementedError

class LLMService:
    async def complete(self, request: LLMRequest) -> LLMResponse:
        raise NotImplementedError

    def stream(self, request: LLMRequest) -> AsyncIterator[str]:
        raise NotImplementedError
```

### Task 1: Project Dependencies, Models, and Core Contracts

**Files:**
- Modify: `pyproject.toml`
- Create: `app/__init__.py`
- Create: `app/model/__init__.py`
- Create: `app/model/request.py`
- Create: `app/model/response.py`
- Create: `app/model/entity.py`
- Create: `app/model/dto.py`
- Create: `app/core/__init__.py`
- Create: `app/core/config.py`
- Create: `app/core/errors.py`
- Create: `app/core/logging.py`
- Create: `app/core/utils.py`
- Test: `tests/test_models.py`

**Interfaces:**
- Consumes: Current Pydantic schemas and `GatewayError` behavior from `gateway.py`.
- Produces: All shared model classes, `GatewayError`, `RetryableProviderError`, application constants, and `encode_sse(event)`.

- [ ] **Step 1: Declare the real project dependencies**

Update `pyproject.toml` so the project can be reproduced with `uv sync --dev`:

```toml
[project]
name = "llmgateway"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = [
    "fastapi>=0.116,<1.0",
    "jsonschema>=4.25,<5.0",
    "openai>=1.109,<3.0",
    "pydantic>=2.11,<3.0",
    "uvicorn[standard]>=0.35,<1.0",
]

[dependency-groups]
dev = [
    "httpx>=0.28,<1.0",
    "pytest>=8.4,<10.0",
    "pytest-asyncio>=1.1,<2.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 2: Write model contract tests before moving production code**

Create `tests/test_models.py` with tests that express the unchanged API contract:

```python
import pytest
from pydantic import ValidationError

from app.core.utils import encode_sse
from app.model.request import LLMRequest, Message


def test_request_rejects_stream_with_response_schema() -> None:
    with pytest.raises(ValidationError, match="stream 与 response_schema 不能同时使用"):
        LLMRequest(
            model="general-primary",
            messages=[Message(role="user", content="hello")],
            stream=True,
            response_schema={"type": "object"},
        )


def test_message_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Message(role="user", content="hello", unknown=True)


def test_encode_sse_preserves_unicode() -> None:
    assert encode_sse({"type": "content.delta", "delta": "你好"}) == (
        'data: {"type": "content.delta", "delta": "你好"}\n\n'
    )
```

- [ ] **Step 3: Run the tests and verify the new modules are missing**

Run: `uv run pytest tests/test_models.py -q`

Expected: FAIL during collection because `app.model.request` and `app.core.utils` do not exist yet.

- [ ] **Step 4: Implement models and core contracts**

Move schemas without changing constraints. Use frozen dataclasses for immutable configuration/value objects:

```python
# app/model/entity.py
@dataclass(frozen=True)
class ModelConfig:
    provider_model: str
    base_url: str
    api_key_env: str
    supports_structured_output: bool
    structured_output_mode: Literal["json_schema", "json_object"] = "json_schema"


@dataclass(frozen=True)
class ModelPrice:
    input_per_million: float
    output_per_million: float
```

Preserve `Message`, `PromptSelection`, `LLMRequest`, `Usage`, `LLMResponse`, `PromptTemplate`, and `CallTrace` field definitions. Add transport error models:

```python
class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    detail: ErrorDetail
```

Implement errors and SSE encoding:

```python
class GatewayError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 502) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class RetryableProviderError(Exception):
    pass


def encode_sse(event: dict[str, Any]) -> str:
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
```

Set `APP_TITLE = "Agent LLM Gateway"`, `APP_VERSION = "0.0.1"`, `BACKUP_MODEL = "general-backup"`, `MAX_ATTEMPTS_PER_MODEL = 2`, and `RETRY_DELAY_SECONDS = 0.1` in `app/core/config.py`.

- [ ] **Step 5: Verify the model tests pass**

Run: `uv run pytest tests/test_models.py -q`

Expected: 3 passed.

- [ ] **Step 6: Review checkpoint**

Run: `Get-ChildItem app/model,app/core -Recurse | Select-Object FullName`

Expected: Only model and core contract files from this task. No Git commit is attempted because this workspace is not a repository.

### Task 2: Mapper and DAO Layers

**Files:**
- Create: `app/mapper/__init__.py`
- Create: `app/mapper/memory_model_mapper.py`
- Create: `app/mapper/memory_prompt_mapper.py`
- Create: `app/mapper/memory_trace_mapper.py`
- Create: `app/mapper/openai_mapper.py`
- Create: `app/dao/__init__.py`
- Create: `app/dao/model_dao.py`
- Create: `app/dao/prompt_dao.py`
- Create: `app/dao/provider_dao.py`
- Create: `app/dao/trace_dao.py`
- Test: `tests/test_data_layers.py`

**Interfaces:**
- Consumes: `ModelConfig`, `ModelPrice`, `PromptTemplate`, `CallTrace`, `ProviderCompletion`, `Message`, `GatewayError`, and `RetryableProviderError`.
- Produces: The four DAO interfaces in Shared Interfaces and their default mapper implementations.

- [ ] **Step 1: Write failing tests for memory persistence boundaries**

Create `tests/test_data_layers.py`:

```python
from datetime import datetime, timezone

from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.trace_dao import TraceDao
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.model.entity import CallTrace


def test_default_model_mapper_exposes_primary_and_backup() -> None:
    dao = ModelDao(MemoryModelMapper())
    assert dao.get_config("general-primary").api_key_env == "DEEPSEEK_API_KEY"
    assert dao.get_config("general-backup").api_key_env == "DEEPSEEK_BACKUP_API_KEY"
    assert dao.get_config("missing") is None


def test_default_prompt_mapper_returns_versioned_template() -> None:
    template = PromptDao(MemoryPromptMapper()).get("knowledge_decision", "v1")
    assert template is not None
    assert "${product_name}" in template.system_template


def test_trace_mapper_returns_a_snapshot() -> None:
    mapper = MemoryTraceMapper()
    dao = TraceDao(mapper)
    trace = CallTrace(
        request_id="request-1",
        timestamp=datetime.now(timezone.utc),
        requested_model="general-primary",
        input_tokens=1,
        output_tokens=2,
        cost_usd=0.0,
        latency_ms=3,
        attempts=1,
        status="success",
    )
    dao.save(trace)
    snapshot = dao.list_all()
    snapshot.clear()
    assert dao.list_all() == [trace]
```

- [ ] **Step 2: Run the tests and verify mapper/DAO modules are missing**

Run: `uv run pytest tests/test_data_layers.py -q`

Expected: FAIL during collection because the mapper and DAO modules do not exist.

- [ ] **Step 3: Implement memory mappers and thin DAOs**

Copy current values exactly into `MemoryModelMapper` and `MemoryPromptMapper`. Read model names/base URLs from the environment when constructing the mapper, matching current import-time behavior. The Trace mapper must return `list(self._traces)` rather than exposing the mutable internal list.

DAO methods must be direct delegation, for example:

```python
class PromptDao:
    def __init__(self, mapper: MemoryPromptMapper) -> None:
        self._mapper = mapper

    def get(self, name: str, version: str) -> PromptTemplate | None:
        return self._mapper.find(name, version)
```

- [ ] **Step 4: Implement the OpenAI mapper and provider DAO**

Move the SDK request construction unchanged. `OpenAIMapper` must convert SDK output into `ProviderCompletion` and translate only temporary transport failures:

```python
except (APIConnectionError, APITimeoutError, RateLimitError, TimeoutError, ConnectionError) as exc:
    raise RetryableProviderError(str(exc)) from exc
```

Missing API keys remain a stable business error:

```python
raise GatewayError("gateway_misconfigured", "Gateway 模型凭据未配置", 503)
```

`ProviderDao` delegates to the mapper; it contains no SDK imports or retry rules.

- [ ] **Step 5: Verify data-layer tests pass**

Run: `uv run pytest tests/test_data_layers.py -q`

Expected: 3 passed.

- [ ] **Step 6: Review checkpoint**

Run: `rg -n "fastapi|APIRouter|HTTPException" app/dao app/mapper`

Expected: no matches. No Git commit is attempted because this workspace is not a repository.

### Task 3: Prompt and Trace Services

**Files:**
- Create: `app/service/__init__.py`
- Create: `app/service/prompt_service.py`
- Create: `app/service/trace_service.py`
- Test: `tests/test_prompt_and_trace_services.py`

**Interfaces:**
- Consumes: `PromptDao`, `ModelDao`, `TraceDao`, request/entity/response models, and the application logger.
- Produces: `PromptService.render`, `PromptService.build_messages`, `TraceService.record`, and `TraceService.list_traces`.

- [ ] **Step 1: Write failing prompt and trace service tests**

Use real memory mappers rather than mocks:

```python
def test_prompt_service_renders_selected_template() -> None:
    service = PromptService(PromptDao(MemoryPromptMapper()))
    message = service.render(
        PromptSelection(name="knowledge_decision", version="v1", variables={"product_name": "Portal"})
    )
    assert message.role == "system"
    assert message.content.startswith("你是Portal的知识库决策器")


def test_prompt_service_rejects_unknown_template() -> None:
    service = PromptService(PromptDao(MemoryPromptMapper()))
    with pytest.raises(GatewayError) as error:
        service.render(PromptSelection(name="missing", version="v1"))
    assert error.value.code == "unknown_prompt_template"
    assert error.value.status_code == 400


def test_prompt_service_rejects_missing_variable() -> None:
    service = PromptService(PromptDao(MemoryPromptMapper()))
    with pytest.raises(GatewayError) as error:
        service.render(PromptSelection(name="knowledge_decision", version="v1"))
    assert error.value.code == "missing_prompt_variable"


def test_trace_service_calculates_cost_and_persists_trace() -> None:
    service = build_trace_service()
    trace = service.record(
        request_id="request-1",
        requested_model="general-primary",
        actual_model="general-primary",
        prompt=None,
        usage=Usage(input_tokens=1_000_000, output_tokens=1_000_000),
        latency_ms=10,
        attempts=1,
        status="success",
    )
    assert trace.cost_usd == 5.0
    assert service.list_traces() == [trace]
```

- [ ] **Step 2: Run tests and verify services are missing**

Run: `uv run pytest tests/test_prompt_and_trace_services.py -q`

Expected: FAIL during collection because service modules do not exist.

- [ ] **Step 3: Implement PromptService**

Use `Template.substitute`, map missing templates and variables to the exact current error codes/messages, and prepend the rendered system message only when `request.prompt` is present. Do not mutate `request.messages`.

- [ ] **Step 4: Implement TraceService**

`record` must build `CallTrace`, calculate cost from `ModelPrice`, save through `TraceDao`, and emit `logger.info("llm_call_trace=%s", trace.model_dump_json())`. Failed calls with `actual_model=None` have zero cost.

- [ ] **Step 5: Verify service tests pass**

Run: `uv run pytest tests/test_prompt_and_trace_services.py -q`

Expected: 4 passed.

- [ ] **Step 6: Review checkpoint**

Run: `rg -n "os\.getenv|AsyncOpenAI|CALL_TRACES|PROMPT_TEMPLATES|MODEL_CONFIGS" app/service`

Expected: no matches. No Git commit is attempted because this workspace is not a repository.

### Task 4: LLM Orchestration Service

**Files:**
- Create: `app/service/llm_service.py`
- Test: `tests/test_llm_service.py`

**Interfaces:**
- Consumes: `ModelDao`, `ProviderDao`, `PromptService`, `TraceService`, request/response models, core errors/config, `jsonschema.validate`, and `encode_sse`.
- Produces: `LLMService.complete(request)` and `LLMService.stream(request)` with current retry/fallback semantics.

- [ ] **Step 1: Write a controllable fake provider mapper for service tests**

Keep the fake in `tests/test_llm_service.py`. It must expose the same methods as `OpenAIMapper`, accept queued completion results/exceptions and queued stream sequences, and record requested provider model names.

```python
class FakeProviderMapper:
    def __init__(self, completions: list[ProviderCompletion | Exception]) -> None:
        self.completions = completions
        self.models: list[str] = []

    async def complete(self, config, messages, timeout_seconds, response_schema):
        self.models.append(config.provider_model)
        result = self.completions.pop(0)
        if isinstance(result, Exception):
            raise result
        return result
```

- [ ] **Step 2: Write failing non-stream orchestration tests**

Cover these exact behaviors in separately named tests: successful provider result and Trace, one temporary retry on the same model, fallback after the second temporary failure, unknown-model 400 error, `use_stream_endpoint` error, invalid JSON, Schema mismatch, and `model_unavailable` Trace after both models fail. For the fallback case, use `[RetryableProviderError("one"), RetryableProviderError("two"), ProviderCompletion("ok", Usage(input_tokens=1, output_tokens=2))]` and assert `response.model == "general-backup"`, `response.attempts == 3`, and the saved Trace uses `actual_model == "general-backup"`.

Assertions must inspect returned model, attempt count, provider call sequence, and saved Trace rather than implementation-private methods.

- [ ] **Step 3: Run the focused tests and verify LLMService is missing**

Run: `uv run pytest tests/test_llm_service.py -q`

Expected: FAIL during collection because `app.service.llm_service` does not exist.

- [ ] **Step 4: Implement non-stream orchestration**

Move the current algorithm into constructor-injected dependencies. Validation remains in Service:

```python
def _validate_model(self, model: str, response_schema: dict[str, Any] | None) -> ModelConfig:
    config = self._model_dao.get_config(model)
    if config is None:
        raise GatewayError("unknown_model", "模型不在 Gateway 允许列表中", 400)
    if response_schema is not None and not config.supports_structured_output:
        raise GatewayError("structured_output_unsupported", "模型不支持 Structured Output", 400)
    return config
```

Retry once only for `RetryableProviderError`. Any final failure moves to the next configured model. Structured output must call `json.loads` then `jsonschema.validate` and preserve `invalid_json` / `schema_validation_failed` errors.

- [ ] **Step 5: Write failing stream orchestration tests**

Cover normal deltas/completion, fallback before the first delta, failure after a delta without fallback duplication, `response_schema` rejection, and unknown model rejection. Assert the exact decoded SSE event order.

- [ ] **Step 6: Implement stream orchestration**

Preserve these invariants:

- The stream method validates request combinations before returning the iterator to the controller.
- A provider failure before any emitted delta may fall back.
- A provider failure after a delta emits one `response.failed` event and stops.
- A successful stream emits `response.completed` with the actual platform model alias.
- Success/failure saves a Trace with zero token counts because the current stream API has no usage result.

- [ ] **Step 7: Verify all LLM service tests pass**

Run: `uv run pytest tests/test_llm_service.py -q`

Expected: all tests in the file pass with no real network calls.

- [ ] **Step 8: Review checkpoint**

Run: `rg -n "AsyncOpenAI|os\.getenv|FastAPI|APIRouter|HTTPException" app/service`

Expected: no matches. No Git commit is attempted because this workspace is not a repository.

### Task 5: Controllers, Global Errors, App Assembly, and Compatibility Entry

**Files:**
- Create: `app/controller/__init__.py`
- Create: `app/controller/llm_controller.py`
- Create: `app/controller/trace_controller.py`
- Create: `app/core/exception_handlers.py`
- Create: `app/main.py`
- Replace: `gateway.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `LLMService`, `TraceService`, model contracts, DAOs, mappers, core errors and configuration.
- Produces: `app.main.app`, `app.main.create_app()`, route factories, and backward-compatible `gateway.app`.

- [ ] **Step 1: Write failing API compatibility tests**

Use `TestClient`, `create_app(llm_service=fake_llm_service, trace_service=fake_trace_service)`, and fake services. Do not call a real provider:

```python
def test_gateway_module_exports_app() -> None:
    from gateway import app
    assert app.title == "Agent LLM Gateway"


def test_gateway_error_is_converted_globally(client) -> None:
    response = client.post("/v1/llm", json=valid_request())
    assert response.status_code == 400
    assert response.json() == {
        "detail": {"code": "unknown_model", "message": "模型不在 Gateway 允许列表中"}
    }
```

Add three separate happy-path tests. The non-stream test asserts the complete JSON returned by `LLMResponse.model_dump(mode="json")`; the stream test asserts `content-type` starts with `text/event-stream` and the response body contains `content.delta` followed by `response.completed`; the Trace test asserts the exact list returned by the fake Trace service.

- [ ] **Step 2: Run API tests and verify app modules are missing**

Run: `uv run pytest tests/test_api.py -q`

Expected: FAIL because `app.main` and the new route factories do not exist.

- [ ] **Step 3: Implement interface-only controller factories**

Each controller exposes a route factory receiving its Service:

```python
def create_llm_router(service: LLMService) -> APIRouter:
    router = APIRouter()

    @router.post("/v1/llm", response_model=LLMResponse)
    async def create_llm_response(request: LLMRequest) -> LLMResponse:
        return await service.complete(request)

    @router.post("/v1/llm/stream")
    async def create_stream(request: LLMRequest) -> StreamingResponse:
        return StreamingResponse(service.stream(request), media_type="text/event-stream")

    return router


def create_trace_router(service: TraceService) -> APIRouter:
    router = APIRouter()

    @router.get("/v1/traces", response_model=list[CallTrace])
    async def list_traces() -> list[CallTrace]:
        return service.list_traces()

    return router
```

Controllers may only declare routers/endpoints, accept validated models, invoke one injected service method, and wrap the async iterator in `StreamingResponse`. They must not import DAO/Mapper modules, catch `GatewayError`, inspect model configuration, or build Prompt/Trace data.

- [ ] **Step 4: Assemble default dependencies in the application composition root**

In `app/main.py`, construct one application-scoped object graph in dependency order:

```text
Memory/OpenAI Mappers -> DAOs -> Prompt/Trace Services -> LLMService
```

Expose `create_app(llm_service: LLMService | None = None, trace_service: TraceService | None = None) -> FastAPI`. When services are omitted, build the default graph once for that application; when supplied, use them for isolated API tests. `app/main.py` is the only composition root allowed to import all layers.

- [ ] **Step 5: Register global error handling and create the app**

Implement a `GatewayError` handler returning the existing `detail` shape. `create_app()` sets the title/version, registers handlers, and includes both routers. Keep validation errors under FastAPI defaults.

- [ ] **Step 6: Replace gateway.py with the compatibility export**

The complete file must be:

```python
from app.main import app

__all__ = ["app"]
```

- [ ] **Step 7: Verify API compatibility tests pass**

Run: `uv run pytest tests/test_api.py -q`

Expected: all API tests pass without network calls.

- [ ] **Step 8: Verify the import and startup target**

Run: `uv run python -c "from gateway import app; print(app.title, app.version)"`

Expected: `Agent LLM Gateway 0.0.1`.

### Task 6: Architecture Guardrails and Project Documentation

**Files:**
- Create: `tests/test_architecture.py`
- Create: `README.md`
- Verify: all project files

**Interfaces:**
- Consumes: The completed module tree and design document.
- Produces: Automated layer-boundary enforcement and operator/developer documentation.

- [ ] **Step 1: Write architecture tests**

Parse Python imports with `ast` and reject forbidden dependencies:

```python
FORBIDDEN_PREFIXES = {
    "controller": ("app.dao", "app.mapper"),
    "service": ("app.controller", "app.mapper"),
    "dao": ("app.controller", "app.service"),
    "mapper": ("app.controller", "app.service", "app.dao"),
    "model": ("app.controller", "app.service", "app.dao", "app.mapper"),
    "core": ("app.controller", "app.service", "app.dao", "app.mapper"),
}
```

Also assert that `gateway.py` contains no classes/functions and exports only `app` from `app.main`.

- [ ] **Step 2: Run architecture tests and fix only actual boundary violations**

Run: `uv run pytest tests/test_architecture.py -q`

Expected: PASS. If it fails, move behavior to the correct layer rather than weakening the forbidden import table.

- [ ] **Step 3: Document operation and capabilities**

Create `README.md` with:

- `uv sync --dev` installation.
- `uv run uvicorn gateway:app --host 0.0.0.0 --port 8000` startup.
- `DEEPSEEK_API_KEY`, `DEEPSEEK_BACKUP_API_KEY`, model and base URL environment variables.
- The three endpoints and concise request examples.
- The six-layer responsibilities and dependency direction.
- Current feature inventory and engineering capability inventory from the spec.
- Current limitations: in-memory Trace/Prompt/config, no auth/rate limiting/health/metrics/persistence.
- PostgreSQL/Redis future responsibilities and the rule that new persistence lives in Mapper implementations.

- [ ] **Step 4: Run the complete test suite**

Run: `uv run pytest -q`

Expected: all tests pass, zero failures and zero collection errors.

- [ ] **Step 5: Compile every Python module**

Run: `uv run python -m compileall -q app gateway.py tests`

Expected: exit code 0 with no syntax errors.

- [ ] **Step 6: Check strict layer imports and residual monolith symbols**

Run: `rg -n "CALL_TRACES|PROMPT_TEMPLATES|MODEL_CONFIGS|class OpenAICompatibleProvider|def call_with_fallback|def stream_with_fallback" gateway.py app`

Expected: legacy symbol names do not remain in `gateway.py`; static data appears only in the appropriate mapper if retained under renamed private constants.

- [ ] **Step 7: Final requirements review**

Compare the implementation to every item under the spec's `完成标准`. Report the exact test count, compile result, preserved startup command, and any verification blocked by unavailable package/network state.
