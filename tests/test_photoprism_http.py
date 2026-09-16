"""Exercise PhotoPrism authentication and response headers over local HTTP."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer
import pytest

from custom_components.album_slideshow import photoprism


_PHOTO = {"UID": "photo-1", "Hash": "hash-1", "Type": "image"}


@asynccontextmanager
async def _client(monkeypatch, responses, **credentials):
    """Serve ordered JSON responses and record the real client requests."""
    calls = []

    async def handle(request):
        response = responses[len(calls)]
        calls.append({
            "method": request.method,
            "path": request.path,
            "authorization": request.headers.get("Authorization"),
            "query": dict(request.query),
            "body": await request.json() if request.method == "POST" else None,
        })
        return web.json_response(
            response["body"],
            status=response.get("status", 200),
            headers=response.get("headers"),
        )

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handle)
    async with TestServer(app) as server, ClientSession() as session:
        monkeypatch.setattr(photoprism, "async_get_clientsession", lambda _hass: session)
        client = photoprism.PhotoprismClient(
            object(), str(server.make_url("/")), **credentials
        )
        yield client, calls
        assert len(calls) == len(responses)


@pytest.mark.parametrize("header_name", [
    "X-Preview-Token", "X-Preview-token", "x-preview-token", "X-PREVIEW-TOKEN",
])
def test_search_accepts_any_preview_header_case(monkeypatch, header_name):
    async def run():
        responses = [{"body": [_PHOTO], "headers": {header_name: "search-preview"}}]
        async with _client(
            monkeypatch, responses, auth_method="app_password", token="app-access"
        ) as (client, calls):
            assert await client.async_collect_assets("all") == [_PHOTO]
            assert client.preview_token == "search-preview"
            assert calls[0]["authorization"] == "Bearer app-access"

    asyncio.run(run())


def test_get_preserves_case_insensitive_headers_after_response_closes(monkeypatch):
    async def run():
        responses = [{"body": [], "headers": {"x-preview-token": "preview"}}]
        async with _client(
            monkeypatch, responses, auth_method="app_password", token="app-access"
        ) as (client, _calls):
            _data, headers = await client._get("/api/v1/photos")
            assert headers.get("X-Preview-Token") == "preview"
            assert headers.get("x-preview-token") == "preview"

    asyncio.run(run())


def test_login_preview_token_survives_headerless_search(monkeypatch):
    async def run():
        responses = [
            {"body": {"access_token": "session-access", "config": {"previewToken": "login-preview"}}},
            {"body": [_PHOTO]},
        ]
        async with _client(
            monkeypatch, responses,
            auth_method="user_password", username="test-user", password="test-password",
        ) as (client, calls):
            assert await client.async_collect_assets("all") == [_PHOTO]
            assert client.preview_token == "login-preview"
            assert calls[0]["path"] == "/api/v1/session"
            assert calls[0]["body"] == {"username": "test-user", "password": "test-password"}
            assert calls[1]["authorization"] == "Bearer session-access"

    asyncio.run(run())


def test_search_header_refreshes_login_preview_token(monkeypatch):
    async def run():
        responses = [
            {"body": {"access_token": "session-access", "config": {"previewToken": "login-preview"}}},
            {"body": [_PHOTO], "headers": {"x-preview-token": "search-preview"}},
        ]
        async with _client(
            monkeypatch, responses,
            auth_method="user_password", username="test-user", password="test-password",
        ) as (client, _calls):
            assert await client.async_collect_assets("all") == [_PHOTO]
            assert client.preview_token == "search-preview"

    asyncio.run(run())


@pytest.mark.parametrize("config", [
    None, {}, [], "invalid", {"previewToken": None}, {"previewToken": ""},
    {"previewToken": 123},
])
def test_missing_or_malformed_login_config_uses_search_header(monkeypatch, config):
    async def run():
        responses = [
            {"body": {"access_token": "session-access", "config": config}},
            {"body": [_PHOTO], "headers": {"X-Preview-Token": "search-preview"}},
        ]
        async with _client(
            monkeypatch, responses,
            auth_method="user_password", username="test-user", password="test-password",
        ) as (client, _calls):
            await client.async_authenticate()
            assert client.preview_token is None
            assert await client.async_collect_assets("all") == [_PHOTO]
            assert client.preview_token == "search-preview"

    asyncio.run(run())


@pytest.mark.parametrize("new_config, expected_preview", [
    ({"previewToken": "renewed-preview"}, "renewed-preview"),
    (None, None),
])
def test_reauthentication_replaces_old_session_preview_token(
    monkeypatch, new_config, expected_preview,
):
    async def run():
        responses = [
            {"body": {"access_token": "old-access", "config": {"previewToken": "old-preview"}}},
            {"status": 401, "body": {"error": "session expired"}},
            {"body": {"access_token": "new-access", "config": new_config}},
            {"body": [_PHOTO]},
        ]
        async with _client(
            monkeypatch, responses,
            auth_method="user_password", username="test-user", password="test-password",
        ) as (client, calls):
            await client.async_authenticate()
            assert client.preview_token == "old-preview"
            assert await client.async_collect_assets("all") == [_PHOTO]
            assert client.preview_token == expected_preview
            assert [(call["method"], call["path"]) for call in calls] == [
                ("POST", "/api/v1/session"), ("GET", "/api/v1/photos"),
                ("POST", "/api/v1/session"), ("GET", "/api/v1/photos"),
            ]
            assert calls[1]["authorization"] == "Bearer old-access"
            assert calls[3]["authorization"] == "Bearer new-access"

    asyncio.run(run())


def test_paginated_search_keeps_last_preview_token(monkeypatch):
    monkeypatch.setattr(photoprism, "_PAGE_SIZE", 1)

    async def run():
        second_photo = {**_PHOTO, "UID": "photo-2"}
        responses = [
            {"body": [_PHOTO], "headers": {"X-Preview-Token": "first-preview"}},
            {"body": [second_photo], "headers": {"x-preview-token": "latest-preview"}},
            {"body": []},
        ]
        async with _client(
            monkeypatch, responses, auth_method="app_password", token="app-access"
        ) as (client, calls):
            assert await client.async_collect_assets("all") == [_PHOTO, second_photo]
            assert client.preview_token == "latest-preview"
            assert [call["query"]["offset"] for call in calls] == ["0", "1", "2"]

    asyncio.run(run())


@pytest.mark.parametrize("response, message", [
    ({"status": 401, "body": {"error": "invalid credentials"}}, "session login failed: 401"),
    ({"body": {"config": {"previewToken": "preview-only"}}}, "no access_token"),
])
def test_login_still_requires_valid_authentication(monkeypatch, response, message):
    async def run():
        async with _client(
            monkeypatch, [response],
            auth_method="user_password", username="test-user", password="test-password",
        ) as (client, _calls):
            with pytest.raises(photoprism.PhotoprismAuthError, match=message):
                await client.async_authenticate()
            assert client.preview_token is None

    asyncio.run(run())