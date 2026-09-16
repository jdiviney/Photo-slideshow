"""Legacy iCloud transport diagnostics and large-album regression coverage."""
from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
import json
import logging
import traceback
from types import SimpleNamespace

from aiohttp import (
    ClientConnectionError, ClientPayloadError, ClientSession, ClientSSLError,
    ClientTimeout, web,
)
from aiohttp.test_utils import TestServer
from multidict import CIMultiDict
import pytest

from custom_components.album_slideshow import coordinator, icloud as ic


_TOKEN = "D2JPrivateTokenX-" + "x" * 62
_HOST = "p143-sharedstreams.icloud.com"


def _photos(count):
    return [
        {
            "photoGuid": f"photo-{i}", "width": 1200, "height": 800,
            "derivatives": {"1200": {"checksum": f"checksum-{i}"}},
        }
        for i in range(count)
    ]


def _asset_reply(photos):
    return _Response({"items": {
        p["derivatives"]["1200"]["checksum"]: {
            "url_location": "cvws.icloud-content.com",
            "url_path": f"/test/{p['photoGuid']}.jpg",
        }
        for p in photos
    }})


class _Response:
    def __init__(self, payload=None, *, status=200, headers=None, error=None, raw=None):
        self.status = status
        self.headers = CIMultiDict(headers or {})
        self.raw = raw if raw is not None else json.dumps(payload or {}).encode()
        self.error = error
        self.closed = False
        self.reads = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.closed = True
        return False

    async def read(self):
        self.reads += 1
        if self.error is not None:
            raise self.error
        return self.raw


class _Session:
    def __init__(self, responses):
        self.responses = deque(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError("Unexpected extra iCloud request")
        response = self.responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response


@pytest.fixture
def install_transport(monkeypatch):
    delays = []

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(ic, "_sleep", fake_sleep, raising=False)

    def install(responses):
        session = _Session(responses)
        monkeypatch.setattr(ic, "async_get_clientsession", lambda _hass: session)
        client = ic.IcloudClient(object(), _TOKEN)
        return client, session, delays

    return install


def _coordinator():
    instance = coordinator.AlbumCoordinator.__new__(coordinator.AlbumCoordinator)
    instance.hass = object()
    instance.entry = SimpleNamespace(
        title="Synthetic",
        data={
            coordinator.CONF_ICLOUD_TOKEN: _TOKEN,
            coordinator.CONF_ICLOUD_BACKEND: ic.BACKEND_SHAREDSTREAMS,
            coordinator.CONF_ICLOUD_IMAGE_SIZE: "full",
        },
    )
    return instance


def test_validation_retries_one_timeout_and_preserves_token(install_transport):
    failed = _Response(error=TimeoutError())
    client, session, delays = install_transport([
        failed, _Response({"streamName": "Test album", "photos": _photos(1)}),
    ])
    assert asyncio.run(client.async_validate()) == "Test album"
    assert len(session.calls) == 2
    assert session.calls[0] == session.calls[1]
    assert f"/{_TOKEN}/sharedstreams/webstream" in session.calls[0][0]
    assert delays == [1.0]
    assert failed.closed


def test_bounded_timeouts_are_passed_to_each_legacy_request(install_transport):
    client, session, _delays = install_transport([_Response({"streamName": "Test"})])
    asyncio.run(client.async_validate())
    options = session.calls[0][1]
    timeout = options["timeout"]
    assert timeout.connect == 15
    assert timeout.sock_read == 60
    assert timeout.total == 90
    assert options["allow_redirects"] is False
    # The separate CloudKit transport keeps its existing timeout.
    assert ic._TIMEOUT == 30


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_exhausted_timeout_names_validation_phase_and_attempt(
    install_transport, caplog, phase,
):
    responses = [
        TimeoutError() if phase == "headers" else _Response(error=TimeoutError())
        for _ in range(2)
    ]
    client, session, delays = install_transport(responses)
    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError) as caught:
        asyncio.run(client.async_validate())
    message = str(caught.value)
    assert "link validation" in message
    assert "/webstream" in message and _HOST in message
    assert "TimeoutError" in message
    assert "attempt=2/2" in message
    assert "elapsed=" in message and "total_elapsed=" in message
    assert ("response headers" if phase == "headers" else "response body") in message
    assert len(session.calls) == 2 and delays == [1.0]
    assert _TOKEN not in message + caplog.text


def test_702_photo_load_retries_only_the_failed_batch(install_transport, caplog):
    photos = _photos(702)
    responses = [_Response({"streamName": "Test", "photos": photos})]
    for start in range(0, len(photos), 25):
        if start == 150:
            responses.append(_Response(error=TimeoutError()))
        responses.append(_asset_reply(photos[start:start + 25]))
    _client, session, delays = install_transport(responses)
    with caplog.at_level(logging.WARNING):
        result = asyncio.run(_coordinator()._update_icloud())
    assert len(result["items"]) == 702
    assert len({item.source_id for item in result["items"]}) == 702
    listings = [call for call in session.calls if call[0].endswith("/webstream")]
    batches = [call for call in session.calls if call[0].endswith("/webasseturls")]
    assert len(listings) == 1
    assert len(batches) == 30  # 29 batches plus the single retry.
    assert batches[6] == batches[7]
    assert [len(call[1]["json"]["photoGuids"]) for call in batches] == [25] * 29 + [2]
    assert delays == [1.0]
    assert "batch 7/29" in caplog.text
    assert _TOKEN not in caplog.text


def test_failed_batch_diagnostic_survives_coordinator_wrapping(install_transport):
    photos = _photos(702)
    responses = [_Response({"photos": photos})]
    responses.extend(_asset_reply(photos[start:start + 25]) for start in range(0, 150, 25))
    responses.extend([_Response(error=TimeoutError()), _Response(error=TimeoutError())])
    _client, session, _delays = install_transport(responses)
    with pytest.raises(coordinator.UpdateFailed) as caught:
        asyncio.run(_coordinator()._update_icloud())
    message = str(caught.value)
    assert message.startswith("Error querying iCloud album: ")
    assert "image URLs batch 7/29 (25 photos)" in message
    assert "TimeoutError" in message and "attempt=2/2" in message
    assert len(session.calls) == 9


@pytest.mark.parametrize("status", [408, 500, 502, 503, 504])
def test_transient_http_status_retries_once(install_transport, status):
    failed = _Response(status=status)
    client, session, delays = install_transport([
        failed, _Response({"streamName": "Recovered"}),
    ])
    assert asyncio.run(client.async_validate()) == "Recovered"
    assert len(session.calls) == 2 and delays == [1.0]
    assert failed.closed and failed.reads == 0


@pytest.mark.parametrize("status", [301, 302, 400, 401, 403, 404, 429])
def test_permanent_http_status_is_not_retried(install_transport, status):
    client, session, delays = install_transport([_Response(status=status)])
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(client.async_validate())
    assert f"HTTP {status}" in str(caught.value)
    assert "link validation" in str(caught.value)
    assert len(session.calls) == 1 and not delays


@pytest.mark.parametrize("error_type", [ClientConnectionError, ClientPayloadError])
def test_transport_errors_do_not_leak_urls_in_logs_or_tracebacks(
    install_transport, caplog, error_type,
):
    secret_url = f"https://{_HOST}/{_TOKEN}/sharedstreams/webstream"
    client, session, delays = install_transport([
        _Response(error=error_type(secret_url)), _Response(error=error_type(secret_url)),
    ])
    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError) as caught:
        asyncio.run(client.async_validate())
    assert error_type.__name__ in str(caught.value)
    trace = "".join(traceback.format_exception(caught.value))
    assert _TOKEN not in str(caught.value) + caplog.text + trace
    assert len(session.calls) == 2 and delays == [1.0]


def test_tls_errors_are_not_retried(install_transport):
    error = ClientSSLError(SimpleNamespace(host=_HOST, port=443, ssl=True), OSError("TLS failed"))
    client, session, delays = install_transport([error])
    with pytest.raises(RuntimeError, match="ClientSSLError"):
        asyncio.run(client.async_validate())
    assert len(session.calls) == 1 and not delays


def test_cancellation_is_not_wrapped_or_retried(install_transport):
    failed = _Response(error=asyncio.CancelledError())
    client, session, delays = install_transport([failed])
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(client.async_validate())
    assert len(session.calls) == 1 and not delays and failed.closed


@pytest.mark.parametrize("raw", [b"not json", b"[]", b"null", b"\xff"])
def test_invalid_json_response_is_safe_and_not_retried(install_transport, raw):
    client, session, delays = install_transport([_Response(raw=raw)])
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(client.async_validate())
    assert "JSON" in str(caught.value) and "link validation" in str(caught.value)
    assert len(session.calls) == 1 and not delays


def test_empty_asset_selection_makes_no_requests(install_transport):
    client, session, delays = install_transport([])
    assert asyncio.run(client.async_get_asset_urls([])) == {}
    assert not session.calls and not delays


@pytest.mark.parametrize("method", ["validate", "photos", "assets"])
def test_json_partition_redirect_is_followed_for_each_operation(install_transport, method):
    photos = _photos(1)
    redirected = _Response({"X-Apple-MMe-Host": "p99-sharedstreams.icloud.com"}, status=330)
    success = (
        _asset_reply(photos) if method == "assets"
        else _Response({"streamName": "Test", "photos": photos})
    )
    client, session, delays = install_transport([redirected, success])
    if method == "validate":
        assert asyncio.run(client.async_validate()) == "Test"
    elif method == "photos":
        assert asyncio.run(client.async_get_photos()) == photos
    else:
        assert len(asyncio.run(client.async_get_asset_urls(["photo-0"]))) == 1
    assert len(session.calls) == 2
    assert session.calls[1][0] == session.calls[0][0].replace(_HOST, "p99-sharedstreams.icloud.com")
    assert session.calls[0][1] == session.calls[1][1]
    assert client._host == "p99-sharedstreams.icloud.com"
    assert not delays and redirected.closed


@pytest.mark.parametrize("header", ["X-Apple-MMe-Host", "x-apple-mme-host", "X-Apple-Mme-Host"])
def test_partition_header_is_case_insensitive_and_allows_empty_body(install_transport, header):
    client, session, _delays = install_transport([
        _Response(status=330, headers={header: "P99-SHAREDSTREAMS.ICLOUD.COM"}, raw=b""),
        _Response({"streamName": "Test"}),
    ])
    assert asyncio.run(client.async_validate()) == "Test"
    assert len(session.calls) == 2 and client._host == "p99-sharedstreams.icloud.com"


@pytest.mark.parametrize("error_type", [TimeoutError, ClientPayloadError])
def test_partition_header_does_not_require_a_readable_body(install_transport, error_type):
    redirect = _Response(
        status=330, headers={"X-Apple-MMe-Host": "p99-sharedstreams.icloud.com"},
        error=error_type(),
    )
    client, session, delays = install_transport([
        redirect, _Response({"streamName": "Test"}),
    ])
    assert asyncio.run(client.async_validate()) == "Test"
    assert redirect.reads == 0 and redirect.closed
    assert len(session.calls) == 2 and not delays


@pytest.mark.parametrize("target", [
    None, ["p99-sharedstreams.icloud.com"],
    "p99-sharedstreams.icloud.com.attacker.example", "attacker.example",
    "https://p99-sharedstreams.icloud.com", "p99-sharedstreams.icloud.com/path",
    "p99-sharedstreams.icloud.com:443", "p99-sharedstreams.icloud.com@attacker.example",
])
def test_partition_redirect_must_be_an_exact_apple_host(install_transport, caplog, target):
    client, session, delays = install_transport([
        _Response({"X-Apple-MMe-Host": target}, status=330),
    ])
    with pytest.raises(RuntimeError, match="invalid or missing partition host"):
        asyncio.run(client.async_validate())
    assert len(session.calls) == 1 and not delays
    assert client._host is None
    assert _TOKEN not in caplog.text


def test_partition_redirect_loop_is_bounded(install_transport):
    client, session, delays = install_transport([
        _Response({"X-Apple-MMe-Host": "p99-sharedstreams.icloud.com"}, status=330),
        _Response({"X-Apple-MMe-Host": "p88-sharedstreams.icloud.com"}, status=330),
    ])
    with pytest.raises(RuntimeError, match="redirect loop"):
        asyncio.run(client.async_validate())
    assert len(session.calls) == 2 and not delays


def test_successful_response_cannot_change_partition(install_transport):
    client, session, delays = install_transport([
        _Response({"streamName": "Test"}, headers={"X-Apple-MMe-Host": "attacker.example"}),
    ])
    assert asyncio.run(client.async_validate()) == "Test"
    assert client._host is None and len(session.calls) == 1 and not delays


def test_redirect_does_not_reset_retry_budget(install_transport):
    client, session, delays = install_transport([
        _Response(error=TimeoutError()),
        _Response({"X-Apple-MMe-Host": "p99-sharedstreams.icloud.com"}, status=330),
        _Response(error=TimeoutError()),
    ])
    with pytest.raises(RuntimeError, match="attempt=2/2"):
        asyncio.run(client.async_validate())
    assert len(session.calls) == 3 and delays == [1.0]


def test_diagnostics_distinguish_attempt_and_total_elapsed(install_transport, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(ic, "monotonic", lambda: clock[0])

    class SlowResponse(_Response):
        async def read(self):
            clock[0] += 60
            raise TimeoutError()

    async def advance_clock(delay):
        clock[0] += delay

    monkeypatch.setattr(ic, "_sleep", advance_clock)
    client, _session, _delays = install_transport([SlowResponse(), SlowResponse()])
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(client.async_validate())
    assert "elapsed=60.0s; total_elapsed=121.0s" in str(caught.value)


def test_cancellation_during_retry_delay_stops_requests(install_transport, monkeypatch):
    async def cancel(_delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(ic, "_sleep", cancel)
    client, session, _delays = install_transport([_Response(error=TimeoutError())])
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(client.async_validate())
    assert len(session.calls) == 1


@asynccontextmanager
async def _local_http_client(monkeypatch, handler):
    """Use aiohttp on loopback, without contacting Apple or depending on HA."""
    app = web.Application()
    app.router.add_post("/{path:.*}", handler)
    async with TestServer(app) as server, ClientSession() as session:
        monkeypatch.setattr(ic, "async_get_clientsession", lambda _hass: session)
        monkeypatch.setattr(
            ic.IcloudClient, "base_url",
            property(lambda self: str(server.make_url(f"/{self.token}/sharedstreams"))),
        )
        yield ic.IcloudClient(object(), _TOKEN)


def test_real_http_status_retry(monkeypatch, install_transport):
    async def run():
        requests = []

        async def handle(request):
            requests.append(await request.json())
            if len(requests) == 1:
                return web.Response(status=503)
            return web.json_response({"streamName": "Recovered"})

        async with _local_http_client(monkeypatch, handle) as client:
            assert await client.async_validate() == "Recovered"
        assert requests == [{"streamCtag": None}] * 2

    asyncio.run(run())


def test_real_stalled_response_body_times_out_twice(monkeypatch, install_transport):
    # Accelerate the transport timeout, not the request logic. No sleeps are
    # used: each server response sends headers, then waits on a cleanup event.
    monkeypatch.setattr(ic, "_LEGACY_TIMEOUT", ClientTimeout(total=2, connect=1, sock_read=0.25))

    async def run():
        release = asyncio.Event()
        requests = []

        async def handle(request):
            requests.append(request.path)
            response = web.StreamResponse()
            await response.prepare(request)
            await release.wait()
            return response

        async with _local_http_client(monkeypatch, handle) as client:
            try:
                with pytest.raises(RuntimeError) as caught:
                    await client.async_validate()
                message = str(caught.value)
                assert "reading response body" in message
                assert "TimeoutError" in message and "attempt=2/2" in message
                assert len(requests) == 2
                assert _TOKEN not in message
            finally:
                release.set()

    asyncio.run(run())