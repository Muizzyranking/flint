import asyncio

import pytest
from pydantic import ValidationError

from app.core.exceptions import HandlerNotFoundException, PayloadValidationException
from app.handlers import (
    get_handler,
    is_registered,
    list_specs,
    registered_handler_names,
    validate_payload,
)
from app.schemas.job import JobCreate


def test_builtin_handlers_are_registered():
    names = registered_handler_names()
    assert "send_email" in names
    assert "webhook_delivery" in names
    assert "log_processing" in names
    for spec in list_specs():
        assert spec.payload_model is not None
        assert spec.json_schema["type"] == "object"


def test_get_handler_returns_a_fresh_instance():
    first = get_handler("send_email")
    second = get_handler("send_email")
    assert first is not second
    assert first.name == "send_email"


def test_unknown_handler_raises_domain_exception():
    with pytest.raises(HandlerNotFoundException) as exc:
        get_handler("nope")
    assert exc.value.status_code == 422
    assert "send_email" in exc.value.message


def test_validate_payload_raises_structured_errors():
    with pytest.raises(PayloadValidationException) as exc:
        validate_payload("send_email", {})
    fields = {error["field"] for error in exc.value.errors}
    assert fields == {"to", "subject"}


def test_job_create_validates_payload_before_insert():
    with pytest.raises(ValidationError) as exc:
        JobCreate(type="send_email", payload={})
    assert "Invalid payload for job type 'send_email'" in str(exc.value)


def test_job_create_rejects_unknown_type():
    with pytest.raises(ValidationError) as exc:
        JobCreate(type="does_not_exist", payload={})
    assert "Unknown job type 'does_not_exist'" in str(exc.value)


def test_job_create_accepts_valid_payload():
    job = JobCreate(
        type="webhook_delivery",
        payload={"url": "https://example.com/hook", "body": {"ok": True}},
        priority=1,
    )
    assert job.type == "webhook_delivery"
    assert job.priority == 1


def test_job_create_rejects_invalid_interval():
    with pytest.raises(ValidationError) as exc:
        JobCreate(
            type="log_processing",
            payload={"lines": ["2026-01-01T00:00:00Z INFO ok"]},
            interval="5x",
        )
    assert "Invalid interval" in str(exc.value)


def test_handler_is_registered():
    assert is_registered("log_processing")
    assert not is_registered("unknown")


def test_email_handler_runs_with_valid_payload(monkeypatch):
    sent = {}

    async def fake_send(message, **kwargs):
        sent["subject"] = message["Subject"]

    monkeypatch.setattr("aiosmtplib.send", fake_send)
    handler = get_handler("send_email")
    result = asyncio.run(
        handler.execute({"to": "user@example.com", "subject": "Hello", "body": "Hi"})
    )
    assert result["delivered"] is True
    assert sent["subject"] == "Hello"
