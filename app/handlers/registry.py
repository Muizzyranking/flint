import inspect
from dataclasses import dataclass, field
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from app.core.exceptions import HandlerNotFoundException, PayloadValidationException
from app.handlers.base import BaseHandler

HandlerT = TypeVar("HandlerT", bound=type[BaseHandler])


@dataclass(frozen=True)
class HandlerSpec:
    name: str
    handler_class: type[BaseHandler]
    payload_model: type[BaseModel]
    description: str = ""
    tags: tuple[str, ...] = ()
    version: str = "1"

    @property
    def json_schema(self) -> dict[str, Any]:
        return self.payload_model.model_json_schema()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "tags": list(self.tags),
            "version": self.version,
            "payload_schema": self.json_schema,
        }


@dataclass
class _Registry:
    specs: dict[str, HandlerSpec] = field(default_factory=dict)


_registry = _Registry()


def register_handler(
    name: str,
    *,
    description: str = "",
    tags: tuple[str, ...] = (),
    version: str = "1",
) -> Any:
    """Class decorator that registers a :class:`BaseHandler` implementation.

    Registration is intentionally static and import-time only. An API request
    can never add, replace or execute new code. The handler class must declare
    a ``payload_model`` class attribute (a Pydantic model) before registration;
    the decorator reads it from the class instead of receiving it as an
    argument, which is what lets ``execute`` receive a typed payload instance.
    """

    def decorator(handler_class: HandlerT) -> HandlerT:
        if not inspect.isclass(handler_class) or not issubclass(
            handler_class, BaseHandler
        ):
            raise TypeError(
                f"{handler_class!r} must subclass app.handlers.base.BaseHandler"
            )
        if not name or name != name.strip():
            raise ValueError("Handler names must be non-empty and trimmed.")
        if name in _registry.specs and _registry.specs[name].handler_class is not (
            handler_class
        ):
            raise ValueError(f"A different handler is already registered as '{name}'.")

        payload_model = getattr(handler_class, "payload_model", None)
        if not (
            isinstance(payload_model, type) and issubclass(payload_model, BaseModel)
        ):
            raise TypeError(
                f"{handler_class.__name__} must declare a `payload_model` class "
                "attribute (a Pydantic BaseModel subclass) before registration."
            )

        handler_class.name = name
        handler_class.description = description or handler_class.description
        handler_class.tags = tags
        handler_class.version = version

        _registry.specs[name] = HandlerSpec(
            name=name,
            handler_class=handler_class,
            payload_model=payload_model,
            description=handler_class.description,
            tags=tags,
            version=version,
        )
        return handler_class

    return decorator


def get_handler(job_type: str) -> BaseHandler:
    """Return a fresh handler instance for *job_type*."""
    spec = _registry.specs.get(job_type)
    if spec is None:
        raise HandlerNotFoundException(job_type, supported=registered_handler_names())
    return spec.handler_class()


def get_spec(job_type: str) -> HandlerSpec | None:
    return _registry.specs.get(job_type)


def list_specs() -> list[HandlerSpec]:
    return sorted(_registry.specs.values(), key=lambda spec: spec.name)


def registered_handler_names() -> list[str]:
    return sorted(_registry.specs)


def is_registered(job_type: str) -> bool:
    return job_type in _registry.specs


def validate_payload(job_type: str, payload: dict[str, Any]) -> BaseModel:
    """
    Validate a payload against the schema declared by *job_type*.

    Raises:
        HandlerNotFoundException: the job type is unknown in this deployment.
        PayloadValidationException: the payload does not match the schema.
    """
    spec = _registry.specs.get(job_type)
    if spec is None:
        raise HandlerNotFoundException(job_type, supported=registered_handler_names())

    try:
        return spec.handler_class.validate_payload(payload)
    except ValidationError as exc:
        errors = []
        for error in exc.errors():
            message = error["msg"]
            if message.startswith("Value error, "):
                message = message.removeprefix("Value error, ")
            errors.append(
                {
                    "field": ".".join(str(part) for part in error["loc"]) or "(root)",
                    "message": message,
                    "type": error["type"],
                }
            )
        raise PayloadValidationException(job_type, errors) from exc
