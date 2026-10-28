import json

from pydantic import BaseModel, Field, field_validator


class SettingsUpdate(BaseModel):
    """
    All fields optional — only provided keys are updated.
    PATCH /api/v1/settings accepts any subset of these.
    """

    dlq_threshold: str | None = Field(
        default=None,
        description="Positive integer string. Default: '5'.",
    )
    alert_emails: str | None = Field(
        default=None,
        description="JSON array string. Example: '[\"admin@example.com\"]'.",
    )

    @field_validator("dlq_threshold")
    @classmethod
    def validate_threshold(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                val = int(v)
                if val < 1:
                    raise ValueError
            except (ValueError, TypeError) as e:
                raise ValueError(
                    "dlq_threshold must be a positive integer string e.g. '5'."
                ) from e
        return v

    @field_validator("alert_emails")
    @classmethod
    def validate_emails(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                parsed = json.loads(v)
                if not isinstance(parsed, list):
                    raise ValueError
                for email in parsed:
                    if not isinstance(email, str) or "@" not in email:
                        raise ValueError(f"Invalid email address: '{email}'")
            except (ValueError, TypeError, json.JSONDecodeError) as e:
                raise ValueError(
                    "alert_emails must be a JSON array of email strings. "
                    'Example: \'["admin@example.com", "oncall@example.com"]\''
                ) from e
        return v
