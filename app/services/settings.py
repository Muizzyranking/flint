import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import SettingNotFoundException
from app.core.logger import get_logger
from app.models.settings import Setting, SettingKey

logger = get_logger(__name__)


async def get_setting(key: str, session: AsyncSession) -> str:
    """
    Return the value string for the given settings key.
    """
    result = await session.execute(select(Setting.value).where(Setting.key == key))
    value = result.scalar_one_or_none()
    if value is None:
        raise SettingNotFoundException(key)
    return value


async def get_setting_with_default(
    key: str,
    default: str,
    db: AsyncSession,
) -> str:
    """
    Return the value for a settings key, falling back to default if not found.
    Useful in worker/scheduler code where a missing key should not crash.
    """
    result = await db.execute(select(Setting.value).where(Setting.key == key))
    value = result.scalar_one_or_none()
    return value if value is not None else default


async def get_all_settings(db: AsyncSession) -> dict[str, str]:
    """
    Return all settings as a flat {key: value} dict.
    """
    result = await db.execute(select(Setting))
    settings = result.scalars().all()
    return {s.key: s.value for s in settings}


async def update_settings(
    updates: dict[str, str],
    db: AsyncSession,
) -> dict[str, str]:
    """
    Upsert one or more settings by key.
    """
    for key, value in updates.items():
        result = await db.execute(select(Setting).where(Setting.key == key))
        existing = result.scalar_one_or_none()

        if existing:
            existing.value = value
            logger.info(
                "setting_updated",
                key=key,
                new_value=value if key != "alert_emails" else "***",
            )
        else:
            new_setting = Setting(key=key, value=value)
            db.add(new_setting)
            logger.info("setting_created", key=key)

    await db.commit()
    return await get_all_settings(db)


async def get_dlq_threshold(db: AsyncSession) -> int:
    """
    Return the DLQ threshold as an integer.
    Falls back to 5 if the setting is missing or unparseable.
    """
    raw = await get_setting_with_default(SettingKey.DLQ_THRESHOLD, "5", db)
    try:
        return int(raw)
    except (ValueError, TypeError):
        logger.warning("dlq_threshold_parse_error", raw_value=raw)
        return 5


async def get_alert_emails(db: AsyncSession) -> list[str]:
    """
    Return the alert email list.
    """
    raw = await get_setting_with_default(SettingKey.ALERT_EMAILS, "[]", db)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(e) for e in parsed if e]
        return []
    except (ValueError, TypeError, json.JSONDecodeError):
        logger.warning("alert_emails_parse_error", raw_value=raw)
        return []


async def get_scheduler_strategy(db: AsyncSession) -> str:
    """
    Return the active scheduler strategy: 'heap' or 'timing_wheel'.
    Falls back to 'heap' if the setting is missing or invalid.
    """
    raw = await get_setting_with_default(SettingKey.SCHEDULER_STRATEGY, "heap", db)
    if raw not in ("heap", "timing_wheel"):
        logger.warning("scheduler_strategy_invalid", raw_value=raw)
        return "heap"
    return raw
