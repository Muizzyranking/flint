from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from app.core.exception_handlers import register_exception_handlers
from app.core.exceptions import (
    JobNotFoundException,
    PayloadValidationException,
)

app = FastAPI()
register_exception_handlers(app)


class Body(BaseModel):
    name: str


@app.get("/missing")
async def missing():
    raise JobNotFoundException("abc")


@app.post("/payload")
async def payload(body: Body):
    return body


@app.get("/payload-error")
async def payload_error():
    raise PayloadValidationException(
        "send_email",
        [{"field": "to", "message": "Field required", "type": "missing"}],
    )


@app.get("/value-error")
async def value_error():
    raise ValueError("bad interval")


@app.get("/http-error")
async def http_error():
    from fastapi import HTTPException

    raise HTTPException(status_code=401, detail="nope")


client = TestClient(app)


def test_domain_exception_uses_global_envelope():
    response = client.get("/missing")
    assert response.status_code == 404
    body = response.json()
    assert body["message"] == "Job 'abc' not found."
    assert body["data"] is None
    assert body["errors"][0]["message"] == body["message"]
    assert body["meta"] is None


def test_request_validation_uses_global_envelope():
    response = client.post("/payload", json={})
    assert response.status_code == 422
    body = response.json()
    assert body["message"] == "Validation failed."
    assert body["errors"][0]["field"] == "body.name"
    assert body["errors"][0]["type"] == "missing"


def test_payload_validation_preserves_field_errors():
    response = client.get("/payload-error")
    assert response.status_code == 422
    body = response.json()
    assert body["errors"][0]["field"] == "to"
    assert body["errors"][0]["type"] == "missing"


def test_value_error_is_not_a_500():
    response = client.get("/value-error")
    assert response.status_code == 422
    assert response.json()["message"] == "bad interval"


def test_http_exception_preserves_status():
    response = client.get("/http-error")
    assert response.status_code == 401
    assert response.json()["message"] == "nope"
