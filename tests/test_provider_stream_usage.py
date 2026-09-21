from types import SimpleNamespace

import pytest

from app.mapper.chat_completions_mapper import ChatCompletionsMapper
from app.mapper.responses_mapper import ResponsesMapper
from app.model.config import ModelRouteConfig, ProviderConfig
from app.model.dto import ProviderRequest
from app.model.request import Message
from app.model.response import Usage


class _AsyncStream:
    def __init__(self, items):
        self._items = iter(items)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self):
        self.closed = True


class _CreateEndpoint:
    def __init__(self, stream):
        self.stream = stream
        self.kwargs = None

    async def create(self, **kwargs):
        self.kwargs = kwargs
        return self.stream


def _provider() -> ProviderConfig:
    return ProviderConfig(
        base_url="https://provider.test",
        api_key_env="TEST_API_KEY",
        supported_protocols={"chat_completions", "responses"},
    )


def _model(protocol: str) -> ModelRouteConfig:
    return ModelRouteConfig.model_validate(
        {
            "provider": "openai",
            "provider_model": "upstream-model",
            "protocol": protocol,
            "capabilities": {
                "streaming": True,
                "structured_output": True,
                "tools": False,
                "multimodal": False,
            },
            "pricing": {"input_per_million": 1, "output_per_million": 2},
        }
    )


def _request() -> ProviderRequest:
    return ProviderRequest(
        messages=[Message(role="user", content="hello")],
        timeout_seconds=10,
    )


@pytest.mark.asyncio
async def test_chat_stream_requests_and_emits_terminal_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _AsyncStream(
        [
            SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        delta=SimpleNamespace(content="hello")
                    )
                ],
                usage=None,
            ),
            SimpleNamespace(
                choices=[],
                usage=SimpleNamespace(
                    prompt_tokens=7,
                    completion_tokens=11,
                ),
            ),
        ]
    )
    endpoint = _CreateEndpoint(stream)
    client = SimpleNamespace(
        chat=SimpleNamespace(completions=endpoint)
    )
    mapper = ChatCompletionsMapper()
    monkeypatch.setattr(mapper, "_create_client", lambda _: client)

    chunks = [
        chunk
        async for chunk in mapper.stream(
            _provider(),
            _model("chat_completions"),
            _request(),
        )
    ]

    assert endpoint.kwargs["stream_options"] == {"include_usage": True}
    assert [chunk.delta for chunk in chunks] == ["hello", None]
    assert chunks[-1].usage == Usage(input_tokens=7, output_tokens=11)
    assert stream.closed is True


@pytest.mark.asyncio
async def test_responses_stream_emits_completed_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = _AsyncStream(
        [
            SimpleNamespace(
                type="response.output_text.delta",
                delta="hello",
            ),
            SimpleNamespace(
                type="response.completed",
                response=SimpleNamespace(
                    usage=SimpleNamespace(input_tokens=5, output_tokens=9)
                ),
            ),
        ]
    )
    endpoint = _CreateEndpoint(stream)
    client = SimpleNamespace(responses=endpoint)
    mapper = ResponsesMapper()
    monkeypatch.setattr(mapper, "_create_client", lambda _: client)

    chunks = [
        chunk
        async for chunk in mapper.stream(
            _provider(),
            _model("responses"),
            _request(),
        )
    ]

    assert [chunk.delta for chunk in chunks] == ["hello", None]
    assert chunks[-1].usage == Usage(input_tokens=5, output_tokens=9)
    assert stream.closed is True
