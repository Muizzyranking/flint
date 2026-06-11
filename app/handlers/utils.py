from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


def parse_payload[T](
    payload: dict[str, Any],
    schema: type[T],
) -> T:
    """
    Validate *payload* against a Pydantic BaseModel subclass.
    """
    try:
        return schema(**payload)
    except ValidationError as exc:
        lines = [f"Payload validation failed for {schema.__name__}:"]
        for err in exc.errors():
            field = " → ".join(str(p) for p in err["loc"]) or "(root)"
            lines.append(f"  • {field}: {err['msg']}  [input={err.get('input')!r}]")
        raise ValueError("\n".join(lines)) from exc
