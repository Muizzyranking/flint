from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import BaseModel


class SettingKey:
    """
    Known setting keys as constants.
    """

    DLQ_THRESHOLD = "dlq_threshold"
    ALERT_EMAILS = "alert_emails"
    SCHEDULER_STRATEGY = "scheduler_strategy"


class Setting(BaseModel):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
    )
    value: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    def __repr__(self) -> str:
        return f"<Setting key={self.key} value={self.value[:30]}>"
