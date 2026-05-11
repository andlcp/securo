"""Download and cache asset logo/favicon images locally.

The yfinance provider returns favicon URLs that point at
`t0.gstatic.com/faviconV2?...`. Loading them from the browser meant:

  - ~6 connections per host serialized the requests on HTTP/1.1 — a
    100-asset portfolio took 30-50 s of waterfall, even though the
    transferred bytes were trivial.
  - Some domains (BR FIIs, smaller B3 stocks) 404 because Google has
    no favicon for them — visible as console errors and pointless
    network noise.
  - Hard dependency on a third-party endpoint for what should be a
    static piece of metadata.

This service fetches the logo once on asset create / refresh and
stores the bytes on the asset row (`logo_data` + `logo_content_type`).
The /api/assets/{id}/icon route then serves it with a 1-year immutable
Cache-Control so the browser caches forever.
"""
import logging
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset

logger = logging.getLogger(__name__)


# How many bytes we accept per logo. Favicons run 1-5 KB; capping at 64 KB
# protects us against an upstream that responds with a full-page HTML
# document (gstatic does this for some 404 paths).
_MAX_LOGO_BYTES = 64 * 1024

# Reasonable timeout — yfinance/gstatic usually respond in <500 ms; if it
# takes longer than a couple seconds it's probably wedged and we'd rather
# move on without caching than block the request that triggered the
# fetch.
_FETCH_TIMEOUT_S = 5.0


async def fetch_and_store_icon(
    session: AsyncSession,
    asset: Asset,
    commit: bool = True,
) -> bool:
    """Download `asset.logo_url` and stash it on the asset row.

    Returns True if a new logo was stored, False if there was nothing to
    fetch or the request failed (caller is expected to keep going either
    way — the cache is best-effort).

    By default commits the session because the most common caller is a
    background task that owns the lifecycle. Pass commit=False to batch
    multiple icon writes in one transaction (the backfill script does).
    """
    if not asset.logo_url:
        return False
    if asset.logo_url.startswith("/api/"):
        # Already pointing at our own endpoint — nothing to fetch.
        return False
    if asset.logo_content_type is not None:
        # Already cached. Refresh flows clear `logo_content_type` (and
        # logo_data) to force a re-fetch.
        return False

    try:
        async with httpx.AsyncClient(
            timeout=_FETCH_TIMEOUT_S, follow_redirects=True,
        ) as client:
            r = await client.get(asset.logo_url)
            if r.status_code != 200:
                logger.info(
                    "icon fetch %s -> %d, skipping cache",
                    asset.logo_url, r.status_code,
                )
                return False
            data = r.content
            if not data or len(data) == 0:
                return False
            if len(data) > _MAX_LOGO_BYTES:
                logger.info(
                    "icon fetch %s returned %d bytes — over the %d cap, skipping",
                    asset.logo_url, len(data), _MAX_LOGO_BYTES,
                )
                return False
            content_type = r.headers.get("content-type") or "image/png"
            # Strip charset parameters; we only care about the MIME type.
            content_type = content_type.split(";")[0].strip()
            # Quick sanity check — gstatic occasionally returns text/html
            # 404 pages with 200 status. Reject anything that isn't image/*.
            if not content_type.startswith("image/"):
                logger.info(
                    "icon fetch %s returned %s, not an image — skipping",
                    asset.logo_url, content_type,
                )
                return False
    except Exception as exc:
        logger.info("icon fetch %s failed: %s", asset.logo_url, exc)
        return False

    asset.logo_data = data
    asset.logo_content_type = content_type
    session.add(asset)
    if commit:
        await session.commit()
    return True


def asset_icon_url(asset_id) -> str:
    """Return the canonical URL the frontend should use for an asset's
    cached icon. Caller is responsible for checking `logo_data is not
    None` first — this just formats the path."""
    return f"/api/assets/{asset_id}/icon"
