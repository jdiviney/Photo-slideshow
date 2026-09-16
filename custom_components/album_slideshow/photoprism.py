"""PhotoPrism (direct API) client and pure parsing helpers.

Talks to a PhotoPrism server using either an app password (used directly as a
Bearer token) or a username + password (exchanged for a session access token).
HTTP lives in ``PhotoprismClient``; the parsing/URL helpers are pure functions
so they can be unit-tested without a live server or aiohttp.

API shape (PhotoPrism ``/api/v1``, Bearer auth):
- ``POST /api/v1/session`` ``{username, password}`` -> ``{access_token,
    config: {previewToken, ...}, ...}`` (only needed for the username +
    password auth method).
- ``GET /api/v1/photos`` ``?count&offset&order=newest&primary=true`` plus a
    filter (``s=<album_uid>``, ``q=subject:<uid>``, ``q=favorite:true``, or a
    custom ``q=``) -> a JSON list of photos. Metadata is inline (``TakenAt``,
    ``Lat``/``Lng``, ``PlaceLabel``, ``Title``, ``Description``, ``Portrait``,
    ``Width``/``Height``), so there is no per-asset enrichment call. The
    response headers carry ``X-Preview-Token`` (for thumbnail URLs) and
    ``X-Count`` (number of items returned, for pagination).
- ``GET /api/v1/albums`` / ``GET /api/v1/subjects`` -> albums / people.
- Image bytes: ``/api/v1/t/<sha1_hash>/<preview_token>/<size>`` - the preview
    token in the URL is enough (cookie-free access by design), so no auth
    header is needed to fetch thumbnails.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import async_timeout
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import PHOTOPRISM_SELECTION_COMPOSITE

_TIMEOUT = 30
_PAGE_SIZE = 1000
_MAX_ASSETS = 20_000

# PhotoPrism media types that render as a still image (videos are skipped).
_IMAGE_TYPES = {"image", "raw", "live", "animated"}


def normalize_base_url(url: str) -> str:
    """Strip trailing slashes and a trailing ``/api``/``/api/v1`` from a URL."""
    u = (url or "").strip().rstrip("/")
    for suffix in ("/api/v1", "/api"):
        if u.endswith(suffix):
            u = u[: -len(suffix)]
    return u.rstrip("/")


def build_image_url(base_url: str, file_hash: str, token: str, size: str) -> str:
    """Build the thumbnail URL for a file hash at the requested size.

    The preview ``token`` is included in the URL on purpose: PhotoPrism serves
    thumbnails cookie-free and gates them with this rotatable token, so no auth
    header is required to fetch the bytes.
    """
    base = normalize_base_url(base_url)
    return f"{base}/api/v1/t/{file_hash}/{token}/{size}"


def _to_epoch_ms(value: Any) -> int | None:
    """Parse an ISO-8601 timestamp to epoch milliseconds, or ``None``."""
    if not isinstance(value, str) or not value:
        return None
    try:
        iso = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return int(dt.timestamp() * 1000)
    except (OverflowError, OSError, ValueError):
        return None


def location_label(city: Any, state: Any, country: Any) -> str | None:
    """Build a short ``"City, Country"`` label from place fields.

    Prefers ``city`` for the locality, falling back to ``state``. Appends the
    country when present. Used only when PhotoPrism's own ``PlaceLabel`` is
    unavailable.
    """
    parts: list[str] = []
    locality = None
    for candidate in (city, state):
        if isinstance(candidate, str) and candidate.strip() and candidate.strip().lower() != "unknown":
            locality = candidate.strip()
            break
    if locality:
        parts.append(locality)
    if isinstance(country, str) and country.strip() and country.strip().lower() not in ("unknown", "zz"):
        parts.append(country.strip())
    return ", ".join(parts) if parts else None


def _is_image(item: Any) -> bool:
    """True for photo items that render as a still image (skip videos)."""
    if not isinstance(item, dict):
        return False
    if not item.get("Hash") or not item.get("UID"):
        return False
    t = str(item.get("Type", "image")).lower()
    return t in _IMAGE_TYPES


def parse_photo_meta(item: dict[str, Any]) -> dict[str, Any]:
    """Extract the metadata we surface from a search photo item."""
    out: dict[str, Any] = {}
    captured = _to_epoch_ms(item.get("TakenAt")) or _to_epoch_ms(item.get("TakenAtLocal"))
    if captured is not None:
        out["captured_at"] = captured
    lat = item.get("Lat")
    lng = item.get("Lng")
    if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
        # 0/0 means "no fix" in PhotoPrism; treat it as no location.
        if not (abs(lat) < 1e-6 and abs(lng) < 1e-6):
            out["latitude"] = float(lat)
            out["longitude"] = float(lng)
    # PhotoPrism builds a human-readable ``PlaceLabel`` (e.g. "San Diego,
    # California, USA"); prefer it, falling back to city/country parts.
    label = item.get("PlaceLabel")
    if not (isinstance(label, str) and label.strip() and label.strip().lower() != "unknown"):
        label = location_label(
            item.get("PlaceCity"), item.get("PlaceState"), item.get("PlaceCountry")
        )
    if isinstance(label, str) and label.strip() and label.strip().lower() != "unknown":
        out["location"] = label.strip()
    # Only surface a real caption. PhotoPrism auto-generates ``Title`` from
    # place + date for every photo, so it would be noise as a caption.
    desc = item.get("Description")
    if isinstance(desc, str) and desc.strip():
        out["description"] = desc.strip()
    return out


def build_query_params(
    selection_type: str, selection_id: str | None
) -> dict[str, str]:
    """Build the search query params for a single (non-composite) member.

    ``album`` uses the ``s`` scope; ``person`` and ``favorites`` use ``q``
    filters; ``search`` passes the user's raw query through; ``all`` adds
    nothing (whole library).
    """
    if selection_type == "album" and selection_id:
        return {"s": selection_id}
    if selection_type == "person" and selection_id:
        return {"q": f"subject:{selection_id}"}
    if selection_type == "favorites":
        return {"q": "favorite:true"}
    if selection_type == "search" and selection_id:
        return {"q": selection_id}
    return {}


def parse_composite_selection(selection_id: str | None) -> dict[str, Any]:
    """Parse a composite selection id into ``{albums, people, favorites}``."""
    albums: list[str] = []
    people: list[str] = []
    favorites = False
    if selection_id:
        try:
            data = json.loads(selection_id)
        except (ValueError, TypeError):
            data = None
        if isinstance(data, dict):
            albums = [a for a in data.get("albums", []) if isinstance(a, str) and a]
            people = [p for p in data.get("people", []) if isinstance(p, str) and p]
            favorites = bool(data.get("favorites"))
    return {"albums": albums, "people": people, "favorites": favorites}


def build_composite_queries(
    selection_id: str | None, filter_query: str | None = None
) -> list[dict[str, str]]:
    """Build one search-param dict per composite union member.

    PhotoPrism has no OR across filters, so each album, person, the favorites
    flag and any custom query becomes its own request; the caller unions the
    results. An empty composite yields a single unfiltered query (the whole
    library).
    """
    sel = parse_composite_selection(selection_id)
    queries: list[dict[str, str]] = []
    for uid in sel["albums"]:
        queries.append({"s": uid})
    for uid in sel["people"]:
        queries.append({"q": f"subject:{uid}"})
    if sel["favorites"]:
        queries.append({"q": "favorite:true"})
    if isinstance(filter_query, str) and filter_query.strip():
        queries.append({"q": filter_query.strip()})
    if not queries:
        queries.append({})
    return queries


class PhotoprismAuthError(Exception):
    """Raised when PhotoPrism authentication fails."""


class PhotoprismClient:
    """Thin async wrapper over the PhotoPrism REST API."""

    def __init__(
        self,
        hass,
        base_url: str,
        *,
        auth_method: str,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.hass = hass
        self.base_url = normalize_base_url(base_url)
        self.auth_method = auth_method
        self._token = token
        self._username = username
        self._password = password
        # Bearer token used for API calls: the app password directly, or the
        # session access token obtained from username + password.
        self._bearer: str | None = token if auth_method == "app_password" else None
        # Login config supplies a preview token even when search headers omit
        # it. Later search responses can refresh it for thumbnail URLs.
        self.preview_token: str | None = None

    @property
    def _headers(self) -> dict[str, str]:
        h = {"Accept": "application/json"}
        if self._bearer:
            h["Authorization"] = "Bearer " + self._bearer
        return h

    async def _login(self) -> None:
        """Exchange username + password for a session access token."""
        session = async_get_clientsession(self.hass)
        async with async_timeout.timeout(_TIMEOUT):
            async with session.post(
                self.base_url + "/api/v1/session",
                json={"username": self._username, "password": self._password},
                headers={"Accept": "application/json"},
            ) as resp:
                if resp.status != 200:
                    raise PhotoprismAuthError(f"session login failed: {resp.status}")
                data = await resp.json()
        token = data.get("access_token") if isinstance(data, dict) else None
        if not token:
            raise PhotoprismAuthError("session login returned no access_token")
        self._bearer = token
        config = data.get("config")
        preview_token = config.get("previewToken") if isinstance(config, dict) else None
        # A new session replaces the old preview token, including on re-login.
        self.preview_token = (
            preview_token.strip() or None if isinstance(preview_token, str) else None
        )

    async def async_authenticate(self) -> None:
        """Ensure a usable Bearer token is available."""
        if self.auth_method == "user_password":
            await self._login()
        elif not self._bearer:
            raise PhotoprismAuthError("no app password configured")

    async def _get(
        self, path: str, params: dict[str, str] | None = None
    ) -> tuple[Any, Mapping[str, str]]:
        """GET JSON and case-insensitive headers, re-authenticating once on 401."""
        if self._bearer is None:
            await self.async_authenticate()
        session = async_get_clientsession(self.hass)
        for attempt in (1, 2):
            async with async_timeout.timeout(_TIMEOUT):
                async with session.get(
                    self.base_url + path, params=params, headers=self._headers
                ) as resp:
                    if resp.status == 401 and attempt == 1 and self.auth_method == "user_password":
                        # Session token likely expired; re-login and retry once.
                        await self._login()
                        continue
                    resp.raise_for_status()
                    return await resp.json(), resp.headers
        raise PhotoprismAuthError("unauthorized after re-authentication")

    async def async_validate(self) -> None:
        """Authenticate and confirm the search endpoint responds."""
        await self.async_authenticate()
        await self._get("/api/v1/photos", {"count": "1", "public": "false"})

    async def async_list_albums(self) -> list[dict[str, Any]]:
        data, _ = await self._get(
            "/api/v1/albums", {"count": "1000", "offset": "0", "type": "album"}
        )
        return data if isinstance(data, list) else []

    async def async_list_people(self) -> list[dict[str, Any]]:
        data, _ = await self._get(
            "/api/v1/subjects", {"count": "1000", "type": "person"}
        )
        return data if isinstance(data, list) else []

    async def async_collect_assets(
        self,
        selection_type: str,
        selection_id: str | None = None,
        filter_query: str | None = None,
    ) -> list[dict[str, Any]]:
        """Collect image photo items for a selection.

        ``composite`` unions albums + people + favorites (+ optional query);
        everything else is a single filtered search. The preview token from the
        responses is captured on ``self.preview_token`` for URL building.
        """
        if selection_type == PHOTOPRISM_SELECTION_COMPOSITE:
            queries = build_composite_queries(selection_id, filter_query)
        else:
            queries = [build_query_params(selection_type, selection_id)]
        return await self._collect_union(queries)

    async def _collect_union(
        self, queries: list[dict[str, str]]
    ) -> list[dict[str, Any]]:
        """Union several searches (OR), deduped by photo UID."""
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for query in queries:
            if len(out) >= _MAX_ASSETS:
                break
            for item in await self._search(query):
                uid = item.get("UID")
                if uid and uid not in seen:
                    seen.add(uid)
                    out.append(item)
        return out

    async def _search(self, query: dict[str, str]) -> list[dict[str, Any]]:
        """Page through ``/api/v1/photos`` for one filter, image items only."""
        collected: list[dict[str, Any]] = []
        offset = 0
        while len(collected) < _MAX_ASSETS:
            params = {
                "count": str(_PAGE_SIZE),
                "offset": str(offset),
                "order": "newest",
                "primary": "true",
                "public": "false",
                "merged": "false",
            }
            params.update(query)
            data, headers = await self._get("/api/v1/photos", params)
            token = headers.get("X-Preview-Token")
            if token:
                self.preview_token = token
            items = data if isinstance(data, list) else []
            for it in items:
                if _is_image(it):
                    collected.append(it)
            if len(items) < _PAGE_SIZE:
                break
            offset += _PAGE_SIZE
        return collected
