from fastapi import FastAPI

from app.controller.llm_controller import create_llm_router
from app.controller.trace_controller import create_trace_router
from app.core.config import APP_TITLE, APP_VERSION
from app.core.exception_handlers import register_exception_handlers
from app.dao.model_dao import ModelDao
from app.dao.prompt_dao import PromptDao
from app.dao.provider_dao import ProviderDao
from app.dao.trace_dao import TraceDao
from app.mapper.memory_model_mapper import MemoryModelMapper
from app.mapper.memory_prompt_mapper import MemoryPromptMapper
from app.mapper.memory_trace_mapper import MemoryTraceMapper
from app.mapper.openai_mapper import OpenAIMapper
from app.service.llm_service import LLMService
from app.service.prompt_service import PromptService
from app.service.trace_service import TraceService


def _build_default_services() -> tuple[LLMService, TraceService]:
    model_dao = ModelDao(MemoryModelMapper())
    prompt_dao = PromptDao(MemoryPromptMapper())
    trace_dao = TraceDao(MemoryTraceMapper())
    provider_dao = ProviderDao(OpenAIMapper())
    prompt_service = PromptService(prompt_dao)
    trace_service = TraceService(model_dao, trace_dao)
    llm_service = LLMService(
        model_dao=model_dao,
        provider_dao=provider_dao,
        prompt_service=prompt_service,
        trace_service=trace_service,
    )
    return llm_service, trace_service


def create_app(
    llm_service: LLMService | None = None,
    trace_service: TraceService | None = None,
) -> FastAPI:
    if llm_service is None or trace_service is None:
        default_llm_service, default_trace_service = _build_default_services()
        llm_service = llm_service or default_llm_service
        trace_service = trace_service or default_trace_service

    application = FastAPI(title=APP_TITLE, version=APP_VERSION)
    register_exception_handlers(application)
    application.include_router(create_llm_router(llm_service))
    application.include_router(create_trace_router(trace_service))
    return application


app = create_app()
