import os
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI

from app.controller.chat_completions_controller import create_chat_completions_router
from app.controller.llm_controller import create_llm_router
from app.controller.model_controller import create_model_router
from app.controller.responses_controller import create_responses_router
from app.controller.session_controller import create_session_router
from app.controller.trace_controller import create_trace_router
from app.core.config import get_gateway_config
from app.core.errors import GatewayError
from app.core.exception_handlers import register_exception_handlers
from app.core.snowflake import SnowflakeIdGenerator
from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.provider_dao import ChatCompletionsDao, ResponsesDao
from app.dao.trace_dao import TraceDao
from app.dao.runtime_dao import RuntimeDao
from app.dao.session_dao import SessionDao
from app.mapper.chat_completions_mapper import ChatCompletionsMapper
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.mapper.responses_mapper import ResponsesMapper
from app.mapper.runtime_mapper import RedisRuntimeMapper
from app.mapper.session_mapper import PostgresSessionMapper
from app.model.config import GatewayConfig
from app.model.enums import LLMProtocolEnum, ModelProviderEnum
from app.service.chat_completions_service import ChatCompletionsService
from app.service.llm_service import LLMService
from app.service.model_service import ModelService
from app.service.prompt_service import PromptService
from app.service.protocol_factory import ProtocolFactory
from app.service.provider.deepseek_provider import DeepseekProvider
from app.service.provider.openai_provider import OpenAIProvider
from app.service.provider_factory import ProviderFactory
from app.service.responses_service import ResponsesService
from app.service.compensation_service import NoOpCompensationService
from app.service.producer_supervisor import ProducerSupervisor
from app.service.stream_session_service import StreamSessionService
from app.service.trace_service import TraceService


@dataclass(frozen=True)
class _ApplicationServices:
    llm: LLMService
    trace: TraceService
    chat_completions: ChatCompletionsService
    responses: ResponsesService
    models: ModelService
    stream_sessions: StreamSessionService


def _build_default_services(config: GatewayConfig) -> _ApplicationServices:
    model_dao = ModelDao(MemoryModelMapper(config))
    prompt_service = PromptService(PromptDao(MemoryPromptMapper()))
    trace_service = TraceService(model_dao, TraceDao(MemoryTraceMapper()))

    protocol_factory = ProtocolFactory(
        {
            LLMProtocolEnum.CHAT_COMPLETIONS: ChatCompletionsDao(
                ChatCompletionsMapper()
            ),
            LLMProtocolEnum.RESPONSES: ResponsesDao(ResponsesMapper()),
        }
    )
    provider_types = {
        ModelProviderEnum.DEEPSEEK: DeepseekProvider,
        ModelProviderEnum.OPENAI: OpenAIProvider,
    }
    provider_factory = ProviderFactory(
        {
            provider: provider_types[provider](provider_config, protocol_factory)
            for provider, provider_config in config.providers.items()
        }
    )
    llm_service = LLMService(
        gateway_config=config,
        provider_factory=provider_factory,
        prompt_service=prompt_service,
        trace_service=trace_service,
    )
    session_config = config.sessions
    session_dao = SessionDao(PostgresSessionMapper(session_config.postgres_dsn_env))
    runtime_dao = RuntimeDao(
        RedisRuntimeMapper(
            session_config.redis_url_env,
            event_ttl_seconds=session_config.event_ttl_seconds,
        )
    )
    supervisor = ProducerSupervisor(
        max_active=session_config.max_active_sessions,
        max_queued=session_config.max_queued_sessions,
        shutdown_grace_seconds=session_config.shutdown_grace_seconds,
    )
    try:
        id_generator = SnowflakeIdGenerator(
            worker_id=int(os.environ[session_config.snowflake_worker_id_env])
        )
    except (KeyError, ValueError) as exc:
        raise GatewayError(
            "gateway_misconfigured",
            "A Snowflake worker ID between 0 and 1023 must be configured",
            503,
        ) from exc
    stream_session_service = StreamSessionService(
        session_dao=session_dao,
        runtime_dao=runtime_dao,
        llm_service=llm_service,
        supervisor=supervisor,
        compensation_service=NoOpCompensationService(),
        config=session_config,
        id_generator=id_generator,
    )
    return _ApplicationServices(
        llm=llm_service,
        trace=trace_service,
        chat_completions=ChatCompletionsService(llm_service),
        responses=ResponsesService(llm_service),
        models=ModelService(config),
        stream_sessions=stream_session_service,
    )


def create_app(
    llm_service: LLMService | None = None,
    trace_service: TraceService | None = None,
    *,
    chat_completions_service: ChatCompletionsService | None = None,
    responses_service: ResponsesService | None = None,
    model_service: ModelService | None = None,
    stream_session_service: StreamSessionService | None = None,
    gateway_config: GatewayConfig | None = None,
) -> FastAPI:
    config = gateway_config or get_gateway_config()
    if any(
        service is None
        for service in (
            llm_service,
            trace_service,
            chat_completions_service,
            responses_service,
            model_service,
            stream_session_service,
        )
    ):
        defaults = _build_default_services(config)
        llm_service = llm_service or defaults.llm
        trace_service = trace_service or defaults.trace
        chat_completions_service = (
            chat_completions_service or defaults.chat_completions
        )
        responses_service = responses_service or defaults.responses
        model_service = model_service or defaults.models
        stream_session_service = stream_session_service or defaults.stream_sessions

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await stream_session_service.start()
        try:
            yield
        finally:
            await stream_session_service.stop()

    application = FastAPI(
        title=config.app.title,
        version=config.app.version,
        lifespan=lifespan,
    )
    register_exception_handlers(application)
    application.include_router(create_llm_router(llm_service))
    application.include_router(create_trace_router(trace_service))
    application.include_router(create_chat_completions_router(chat_completions_service))
    application.include_router(create_responses_router(responses_service))
    application.include_router(create_model_router(model_service))
    application.include_router(create_session_router(stream_session_service))
    return application


app = create_app()
