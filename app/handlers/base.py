from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import BaseModel


class BaseHandler[PayloadT: BaseModel](ABC):
    """
    Base class for a job handler.

    A handler is *code*, not runtime state. It is registered with
    :func:`app.handlers.registry.register_handler` when the process starts.
    The API never accepts handler code at runtime.

    Subclasses declare their payload schema on ``payload_model`` and are
    parameterised by it: ``execute`` receives a validated payload instance,
    never a raw dict.
    """

    name: ClassVar[str] = ""
    description: ClassVar[str] = ""
    payload_model: ClassVar[type[BaseModel]]
    tags: ClassVar[tuple[str, ...]] = ()
    version: ClassVar[str] = "1"

    @abstractmethod
    async def execute(self, payload: PayloadT) -> dict[str, Any]:
        """Execute the job logic and return a JSON-serialisable result."""
        ...

    @classmethod
    def validate_payload(cls, payload: dict[str, Any]) -> BaseModel:
        """Validate *payload* against the handler's declared Pydantic model."""
        return cls.payload_model.model_validate(payload)

    @property
    def handler_name(self) -> str:
        """Human-readable name for logging."""
        return self.__class__.__name__
