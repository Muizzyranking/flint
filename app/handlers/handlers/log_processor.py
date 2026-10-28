from collections import Counter
from typing import Any

from pydantic import BaseModel, Field, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.registry import register_handler

logger = get_logger(__name__)

VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
TOP_ERRORS_LIMIT = 5
MAX_LINES = 10_000
MAX_LINE_LENGTH = 8_000


class LogPayload(BaseModel):
    lines: list[str] = Field(
        description="Log lines formatted as '<timestamp> <LEVEL> <message>'."
    )
    source: str = Field(default="unknown", description="Optional source label.")

    @field_validator("lines")
    @classmethod
    def validate_lines(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("'lines' must be a non-empty list.")
        if len(value) > MAX_LINES:
            raise ValueError(f"'lines' must contain at most {MAX_LINES} entries.")
        if any(len(line) > MAX_LINE_LENGTH for line in value):
            raise ValueError(
                f"Each log line must be at most {MAX_LINE_LENGTH} characters."
            )
        return value


@register_handler(
    "log_processing",
    description="Parse and aggregate a batch of application log lines.",
    tags=("analysis", "logs"),
)
class LogProcessorHandler(BaseHandler[LogPayload]):
    payload_model = LogPayload

    async def execute(self, payload: LogPayload) -> dict[str, Any]:
        lines: list[str] = payload.lines
        source: str = payload.source

        parsed = []
        parse_errors = []

        for raw_line in lines:
            line = str(raw_line).strip()
            if not line:
                continue

            parts = line.split(None, 2)
            if len(parts) < 2:
                parse_errors.append(line)
                continue

            timestamp = parts[0]
            level = parts[1].upper()
            message = parts[2] if len(parts) == 3 else ""

            if level not in VALID_LEVELS:
                parse_errors.append(line)
                continue

            parsed.append(
                {
                    "timestamp": timestamp,
                    "level": level,
                    "message": message,
                }
            )

        if not parsed:
            raise ValueError(
                f"No parseable log lines found in payload. "
                f"Expected format: '<timestamp> <LEVEL> <message>' "
                f"where LEVEL is one of: {', '.join(sorted(VALID_LEVELS))}. "
                f"{len(parse_errors)} line(s) could not be parsed."
            )

        level_counts = Counter(entry["level"] for entry in parsed)

        error_messages = [
            entry["message"]
            for entry in parsed
            if entry["level"] in ("ERROR", "CRITICAL")
        ]
        top_errors = [
            {"message": msg, "count": count}
            for msg, count in Counter(error_messages).most_common(TOP_ERRORS_LIMIT)
        ]

        error_count = level_counts.get("ERROR", 0) + level_counts.get("CRITICAL", 0)
        error_rate = round((error_count / len(parsed)) * 100, 2) if parsed else 0.0

        summary = {
            "source": source,
            "total_lines": len(lines),
            "parsed_lines": len(parsed),
            "parse_errors": len(parse_errors),
            "level_counts": dict(level_counts),
            "top_errors": top_errors,
            "has_critical": level_counts.get("CRITICAL", 0) > 0,
            "error_rate_pct": error_rate,
        }

        logger.info(
            "log_processing_complete",
            source=source,
            total_lines=len(lines),
            parsed_lines=len(parsed),
            parse_errors=len(parse_errors),
            has_critical=summary["has_critical"],
            error_rate_pct=error_rate,
        )

        return summary
