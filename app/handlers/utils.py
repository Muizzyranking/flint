import asyncio
import ipaddress
import socket
from urllib.parse import urlparse

ALLOWED_OUTBOUND_PORTS = {80, 443}


async def assert_safe_outbound_url(url: str) -> None:
    """
    Reject URLs that could reach internal networks (basic SSRF guard).

    This is intentionally conservative: only HTTP(S), public IP addresses and
    standard web ports are allowed. Redirects are disabled by the caller, which
    closes the most common redirect-to-metadata escape hatch.
    """
    parsed = urlparse(url)
    _validate_http_url(url)

    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    try:
        addresses = await asyncio.to_thread(
            socket.getaddrinfo,
            parsed.hostname,
            port,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise ValueError(f"Could not resolve host '{parsed.hostname}'.") from exc

    for _family, _, _, _, sockaddr in addresses:
        ip = ipaddress.ip_address(sockaddr[0])
        if not ip.is_global:
            raise ValueError(
                f"Host '{parsed.hostname}' resolves to a non-public "
                f"address ({ip}), which is blocked."
            )


def validate_outbound_url_syntax(url: str) -> str:
    """Validate URL shape and literal addresses at admission time.

    DNS names cannot be resolved safely during request validation (that would
    make admission depend on external DNS). The execution path resolves them
    again in :func:`assert_safe_outbound_url`.
    """
    _validate_http_url(url)
    return url


def _validate_http_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL must use http or https.")
    if not parsed.hostname:
        raise ValueError("URL must include a hostname.")
    if parsed.username or parsed.password:
        raise ValueError("URLs must not contain credentials.")

    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("URL contains an invalid port.") from exc
    if port is not None and port not in ALLOWED_OUTBOUND_PORTS:
        raise ValueError(
            f"Port {port} is not allowed. Use one of: {sorted(ALLOWED_OUTBOUND_PORTS)}."
        )

    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        return
    if not address.is_global:
        raise ValueError(f"Host '{parsed.hostname}' is not a public address.")
