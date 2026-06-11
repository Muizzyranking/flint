from abc import ABC, abstractmethod
from typing import Any


class BaseHandler(ABC):
    @abstractmethod
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the job logic.
        """
        ...

    @property
    def handler_name(self) -> str:
        """Human-readable name for logging."""
        return self.__class__.__name__
