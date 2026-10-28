import os

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger

logger = get_logger(__name__)

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "templates", "email")

jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)


async def send_dlq_alert(
    dlq_count: int,
    session: AsyncSession,
) -> None:
    """
    Send a DLQ threshold alert email to all configured recipients.
    """
    from app.core.config import settings as app_settings
    from app.core.email import EmailMessage, send_email
    from app.services.dlq import get_recent_dlq_jobs
    from app.services.settings import get_alert_emails, get_dlq_threshold

    recipients = await get_alert_emails(session)
    if not recipients:
        logger.info(
            "dlq_alert_skipped",
            reason="no_recipients_configured",
            dlq_count=dlq_count,
        )
        return

    threshold = await get_dlq_threshold(session)
    recent_jobs = await get_recent_dlq_jobs(session, limit=10)

    try:
        template = jinja_env.get_template("dlq_alert.html")
        html_body = template.render(
            dlq_count=dlq_count,
            threshold=threshold,
            jobs=recent_jobs,
            dashboard_url=app_settings.FRONTEND_URL.rstrip("/") + "/dlq",
            app_name="Flint",
            tagline="Quietly igniting your payload, every job has a spark.",
        )
    except Exception as exc:
        logger.error(
            "dlq_alert_template_error",
            error=str(exc),
        )
        html_body = _plain_text_fallback(dlq_count, threshold, recent_jobs)

    subject = (
        f"[Flint Alert] DLQ threshold reached — "
        f"{dlq_count} failed job{'s' if dlq_count != 1 else ''}"
    )

    for recipient in recipients:
        try:
            await send_email(
                EmailMessage(
                    to=recipient,
                    subject=subject,
                    body="Flint DLQ Alert: threshold reached. "
                    "Visit your dashboard for details.",
                    html=html_body,
                )
            )
            logger.info("dlq_alert_email_delivered", to=recipient)
        except Exception as exc:
            logger.error(
                "dlq_alert_email_failed",
                to=recipient,
                error=str(exc),
            )

    logger.warning(
        "dlq_alert_sent",
        dlq_count=dlq_count,
        threshold=threshold,
        recipient_count=len(recipients),
    )


def _plain_text_fallback(
    dlq_count: int,
    threshold: int,
    jobs: list[dict],
) -> str:
    """
    Plain HTML fallback when the Jinja template fails to render.
    """
    rows = "".join(
        f"<tr>"
        f"<td>{j['short_id']}</td>"
        f"<td>{j['type']}</td>"
        f"<td>{j['last_error'][:80]}</td>"
        f"<td>{j['retry_count']}/{j['max_retries']}</td>"
        f"<td>{j['updated_at']}</td>"
        f"</tr>"
        for j in jobs
    )
    return f"""
    <html><body>
    <h2>Flint — DLQ Threshold Reached</h2>
    <p>{dlq_count} failed jobs in the DLQ (threshold: {threshold}).</p>
    <table border="1" cellpadding="4">
      <tr><th>ID</th><th>Type</th><th>Error</th><th>Retries</th><th>Failed At</th></tr>
      {rows}
    </table>
    </body></html>
    """
