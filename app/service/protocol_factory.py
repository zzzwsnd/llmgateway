from collections.abc import Mapping

from app.core.errors import GatewayError
from app.dao.provider_dao import ProviderDao
from app.model.enums import LLMProtocolEnum


class ProtocolFactory:
    def __init__(self, protocols: Mapping[LLMProtocolEnum, ProviderDao]) -> None:
        if any(not isinstance(protocol, LLMProtocolEnum) for protocol in protocols):
            raise TypeError("protocol registrations must use LLMProtocolEnum keys")
        self._protocols: dict[LLMProtocolEnum, ProviderDao] = dict(protocols)

    def get(self, protocol: LLMProtocolEnum) -> ProviderDao:
        dao = self._protocols.get(protocol)
        if dao is None:
            raise GatewayError("protocol_not_registered", "Protocol is not registered", 500)
        return dao
