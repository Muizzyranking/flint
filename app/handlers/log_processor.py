"""
Payload shape:
    {
        "lines": [
            "2026-06-09T10:00:00Z INFO  Request received",
            "2026-06-09T10:00:01Z ERROR Database timeout",
            "2026-06-09T10:00:02Z ERROR Database timeout"
        ],
        "source": "app-server-01"   # optional label
    }

Result shape (on success):
    {
        "source": "app-server-01",
        "total_lines": 3,
        "parsed_lines": 3,
        "parse_errors": 0,
        "level_counts": {"INFO": 1, "ERROR": 2},
        "top_errors": [
            {"message": "Database timeout", "count": 2}
        ],
        "has_critical": false,
        "error_rate_pct": 66.67
    }
"""

from collections import Counter
from typing import Any

from pydantic import BaseModel, field_validator

from app.core.logger import get_logger
from app.handlers.base import BaseHandler
from app.handlers.utils import parse_payload

logger = get_logger(__name__)

VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
TOP_ERRORS_LIMIT = 5


class LogPayload(BaseModel):
    lines: list[str]
    source: str = "unknown"

    @field_validator("lines")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("'lines' must be a non-empty list.")
        return v


class LogProcessorHandler(BaseHandler):
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        p = parse_payload(payload, LogPayload)
        lines: list = p.lines
        source: str = p.source

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

            # Parts: [timestamp, level] or [timestamp, level, message]
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

        # Aggregate
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
