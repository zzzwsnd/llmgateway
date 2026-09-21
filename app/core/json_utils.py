import json
import re
from collections.abc import Iterable
from typing import Any

from jsonschema import SchemaError, ValidationError
from jsonschema.validators import validator_for

from app.core.errors import InvalidJsonSchemaError, JsonOutputValidationError
from app.model.config import JsonParsingConfig
from app.model.dto import JsonParseResult


_STRUCTURAL_TOKEN = re.compile(r'"(?:\\.|[^"\\])*"|[{}\[\]]')
_TRAILING_WHITESPACE = re.compile(r"\s*$")
_PATH_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CLOSING_BRACKET = {"{": "}", "[": "]"}


class JsonOutputParser:
    def __init__(self, config: JsonParsingConfig) -> None:
        self._retry_prompt = config.retry_prompt.active.template

    def parse(self, content: str, schema: dict[str, Any]) -> JsonParseResult:
        normalized_content = content
        repaired = False
        try:
            value = json.loads(content)
        except json.JSONDecodeError as original_error:
            normalized_content = _complete_trailing_brackets(content)
            if normalized_content is None:
                raise self._invalid_json_error(schema) from original_error
            try:
                value = json.loads(normalized_content)
            except json.JSONDecodeError as repaired_error:
                raise self._invalid_json_error(schema) from repaired_error
            repaired = True

        validator_type = self._validator_type(schema)
        errors = sorted(
            validator_type(schema).iter_errors(value),
            key=lambda error: (
                tuple(str(part) for part in error.absolute_path),
                str(error.validator),
            ),
        )
        if errors:
            missing_parameters, invalid_parameters = _classify_errors(errors)
            raise JsonOutputValidationError(
                "schema_validation_failed",
                self._render_retry_prompt(
                    schema,
                    missing_parameters=missing_parameters,
                    invalid_parameters=invalid_parameters,
                ),
                missing_parameters=missing_parameters,
                invalid_parameters=invalid_parameters,
            )

        return JsonParseResult(
            content=normalized_content,
            value=value,
            repaired=repaired,
        )

    def validate_schema(self, schema: dict[str, Any]) -> None:
        self._validator_type(schema)

    @staticmethod
    def _validator_type(schema: dict[str, Any]):
        try:
            validator_type = validator_for(schema)
            validator_type.check_schema(schema)
        except SchemaError as exc:
            raise InvalidJsonSchemaError from exc
        return validator_type

    def _invalid_json_error(
        self, schema: dict[str, Any]
    ) -> JsonOutputValidationError:
        return JsonOutputValidationError(
            "invalid_json",
            self._render_retry_prompt(
                schema,
                json_error="JSON 语法错误，无法自动修复",
            ),
        )

    def _render_retry_prompt(
        self,
        schema: dict[str, Any],
        *,
        missing_parameters: tuple[str, ...] = (),
        invalid_parameters: tuple[str, ...] = (),
        json_error: str | None = None,
    ) -> str:
        return self._retry_prompt.format(
            missing_parameters="、".join(missing_parameters) or "无",
            invalid_parameters="；".join(invalid_parameters) or "无",
            json_error=json_error or "无",
            schema=json.dumps(schema, ensure_ascii=False, separators=(",", ":")),
        )


def _complete_trailing_brackets(content: str) -> str | None:
    stack: list[str] = []
    for match in _STRUCTURAL_TOKEN.finditer(content):
        token = match.group()
        if token.startswith('"'):
            continue
        if token in _CLOSING_BRACKET:
            stack.append(token)
            continue
        if not stack or _CLOSING_BRACKET[stack[-1]] != token:
            return None
        stack.pop()

    if not stack:
        return None
    suffix = "".join(_CLOSING_BRACKET[token] for token in reversed(stack))
    return _TRAILING_WHITESPACE.sub(suffix, content, count=1)


def _classify_errors(
    errors: Iterable[ValidationError],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    missing: list[str] = []
    invalid: list[str] = []
    for error in errors:
        path = _json_path(error.absolute_path)
        if error.validator == "required" and isinstance(error.instance, dict):
            missing.extend(
                _child_path(path, str(name))
                for name in error.validator_value
                if name not in error.instance
            )
            continue
        invalid.append(f"{path}: {error.message}")
    return tuple(missing), tuple(invalid)


def _json_path(parts: Iterable[str | int]) -> str:
    path = "$"
    for part in parts:
        path = _child_path(path, part)
    return path


def _child_path(path: str, part: str | int) -> str:
    if isinstance(part, int):
        return f"{path}[{part}]"
    if _PATH_IDENTIFIER.fullmatch(part):
        return f"{path}.{part}"
    return f"{path}[{json.dumps(part, ensure_ascii=False)}]"
