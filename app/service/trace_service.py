from datetime import datetime, timezone
from typing import Literal

from app.core.logging import logger
from app.dao.model_dao import ModelDao
from app.dao.trace_dao import TraceDao
from app.model.entity import CallTrace
from app.model.enums import ModelEnum
from app.model.request import PromptSelection
from app.model.response import Usage


class TraceService:
    def __init__(self, model_dao: ModelDao, trace_dao: TraceDao) -> None:
        self._model_dao = model_dao
        self._trace_dao = trace_dao

    def record(
        self,
        request_id: str,
        requested_model: ModelEnum,
        actual_model: ModelEnum | None,
        prompt: PromptSelection | None,
        usage: Usage,
        latency_ms: int,
        attempts: int,
        status: Literal["success", "failed"],
        error_code: str | None = None,
    ) -> CallTrace:
        trace = CallTrace(
            request_id=request_id,
            timestamp=datetime.now(timezone.utc),
            requested_model=requested_model,
            actual_model=actual_model,
            prompt_name=prompt.name if prompt else None,
            prompt_version=prompt.version if prompt else None,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cost_usd=self._calculate_cost(actual_model, usage),
            latency_ms=latency_ms,
            attempts=attempts,
            status=status,
            error_code=error_code,
        )
        self._trace_dao.save(trace)
        logger.info("llm_call_trace=%s", trace.model_dump_json())
        return trace

    def list_traces(self) -> list[CallTrace]:
        return self._trace_dao.list_all()

    def _calculate_cost(self, model: ModelEnum | None, usage: Usage) -> float:
        if model is None:
            return 0
        price = self._model_dao.get_price(model)
        if price is None:
            raise KeyError(model)
        return (
            usage.input_tokens * price.input_per_million
            + usage.output_tokens * price.output_per_million
        ) / 1_000_000
