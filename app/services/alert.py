import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiosmtplib
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger

logger = get_logger(__name__)

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "templates", "emails")

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
            dashboard_url="https://app.flint.local/dlq",
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
        await _send_email(
            to=recipient,
            subject=subject,
            html_body=html_body,
            smtp_host=app_settings.SMTP_HOST,
            smtp_port=app_settings.SMTP_PORT,
            smtp_from=app_settings.SMTP_FROM,
        )

    logger.warning(
        "dlq_alert_sent",
        dlq_count=dlq_count,
        threshold=threshold,
        recipient_count=len(recipients),
    )


async def _send_email(
    to: str,
    subject: str,
    html_body: str,
    smtp_host: str,
    smtp_port: int,
    smtp_from: str,
) -> None:
    """Send a single HTML email via aiosmtplib."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to

    # Attach plain text fallback first, then HTML
    plain = "Flint DLQ Alert: threshold reached. Visit your dashboard for details."
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        await aiosmtplib.send(
            msg,
            hostname=smtp_host,
            port=smtp_port,
            use_tls=False,
            start_tls=False,
        )
        logger.info("dlq_alert_email_delivered", to=to)
    except Exception as exc:
        logger.error(
            "dlq_alert_email_failed",
            to=to,
            error=str(exc),
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
