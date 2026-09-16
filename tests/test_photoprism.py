from __future__ import annotations

import asyncio

from custom_components.album_slideshow import photoprism


# ── normalize_base_url ─────────────────────────────────────────────────────

def test_normalize_strips_trailing_slash_and_api():
    assert photoprism.normalize_base_url("http://x:2342/") == "http://x:2342"
    assert photoprism.normalize_base_url("http://x:2342/api") == "http://x:2342"
    assert photoprism.normalize_base_url("http://x:2342/api/v1") == "http://x:2342"
    assert photoprism.normalize_base_url("http://x:2342/api/v1/") == "http://x:2342"
    assert photoprism.normalize_base_url("http://x:2342") == "http://x:2342"


# ── build_image_url ────────────────────────────────────────────────────────

def test_build_image_url():
    u = photoprism.build_image_url("http://x:2342", "abc123", "tok", "fit_1920")
    assert u == "http://x:2342/api/v1/t/abc123/tok/fit_1920"


def test_build_image_url_strips_api_suffix():
    u = photoprism.build_image_url("http://x:2342/api/v1/", "h", "t", "fit_1280")
    assert u == "http://x:2342/api/v1/t/h/t/fit_1280"


# ── _to_epoch_ms ───────────────────────────────────────────────────────────

def test_to_epoch_ms_parses_z():
    assert photoprism._to_epoch_ms("2026-07-02T20:13:55Z") == 1783023235000


def test_to_epoch_ms_bad():
    assert photoprism._to_epoch_ms(None) is None
    assert photoprism._to_epoch_ms("nonsense") is None


# ── location_label ─────────────────────────────────────────────────────────

def test_location_label_city_country():
    assert photoprism.location_label("Lisbon", None, "Portugal") == "Lisbon, Portugal"


def test_location_label_skips_unknown():
    assert photoprism.location_label("Unknown", None, "zz") is None
    assert photoprism.location_label(None, "California", None) == "California"


# ── parse_photo_meta ───────────────────────────────────────────────────────

def test_parse_photo_meta_prefers_place_label():
    item = {
        "TakenAt": "2026-07-02T20:13:55Z",
        "Lat": 32.95, "Lng": -117.22,
        "PlaceLabel": "San Diego, California, USA",
        "PlaceCity": "San Diego", "PlaceCountry": "us",
        "Description": "A caption",
        "Title": "San Diego / United States / 2026",
    }
    meta = photoprism.parse_photo_meta(item)
    assert meta["captured_at"] == 1783023235000
    assert meta["latitude"] == 32.95 and meta["longitude"] == -117.22
    assert meta["location"] == "San Diego, California, USA"
    # Real Description wins; the auto-generated Title is not used as a caption.
    assert meta["description"] == "A caption"


def test_parse_photo_meta_no_title_fallback():
    item = {"Title": "Auto Title / 2026", "Description": None}
    meta = photoprism.parse_photo_meta(item)
    assert "description" not in meta


def test_parse_photo_meta_zero_gps_is_none():
    item = {"Lat": 0.0, "Lng": 0.0}
    meta = photoprism.parse_photo_meta(item)
    assert "latitude" not in meta and "longitude" not in meta


# ── build_query_params ─────────────────────────────────────────────────────

def test_build_query_params_album():
    assert photoprism.build_query_params("album", "al1") == {"s": "al1"}


def test_build_query_params_person():
    assert photoprism.build_query_params("person", "p1") == {"q": "subject:p1"}


def test_build_query_params_favorites():
    assert photoprism.build_query_params("favorites", None) == {"q": "favorite:true"}


def test_build_query_params_all():
    assert photoprism.build_query_params("all", None) == {}


# ── composite selection ────────────────────────────────────────────────────

def test_parse_composite_selection():
    sel = photoprism.parse_composite_selection(
        '{"albums": ["a"], "people": ["p1", "p2"], "favorites": true}'
    )
    assert sel == {"albums": ["a"], "people": ["p1", "p2"], "favorites": True}


def test_parse_composite_selection_bad():
    assert photoprism.parse_composite_selection("nope") == {
        "albums": [], "people": [], "favorites": False,
    }


def test_build_composite_queries_mixes():
    qs = photoprism.build_composite_queries(
        '{"albums": ["a1"], "people": ["p1"], "favorites": true}'
    )
    assert {"s": "a1"} in qs
    assert {"q": "subject:p1"} in qs
    assert {"q": "favorite:true"} in qs
    assert len(qs) == 3


def test_build_composite_queries_with_filter():
    qs = photoprism.build_composite_queries(
        '{"albums": [], "people": [], "favorites": false}', "color:red"
    )
    assert qs == [{"q": "color:red"}]


def test_build_composite_queries_empty_is_all():
    assert photoprism.build_composite_queries(None) == [{}]


# ── union collect (mocked _search) ─────────────────────────────────────────

def _photo(uid, hash_="h", type_="image"):
    return {"UID": uid, "Hash": hash_, "Type": type_}


class _FakeClient(photoprism.PhotoprismClient):
    """Client whose _search is stubbed with canned results per query."""

    def __init__(self, results_by_key):
        self._results = results_by_key
        self.calls = []
        self.preview_token = "tok"

    async def _search(self, query):
        self.calls.append(dict(query))
        if "s" in query:
            key = "album:" + query["s"]
        elif query.get("q", "").startswith("subject:"):
            key = "person:" + query["q"].split(":", 1)[1]
        elif query.get("q") == "favorite:true":
            key = "favorites"
        else:
            key = "all"
        return self._results.get(key, [])


def test_composite_union_dedupes_by_uid():
    c = _FakeClient({
        "album:a1": [_photo("a"), _photo("shared")],
        "person:p1": [_photo("shared"), _photo("b")],
        "favorites": [_photo("c")],
    })
    out = asyncio.run(
        c.async_collect_assets("composite", '{"albums":["a1"],"people":["p1"],"favorites":true}')
    )
    assert [p["UID"] for p in out] == ["a", "shared", "b", "c"]


def test_composite_empty_fetches_all():
    c = _FakeClient({"all": [_photo("a"), _photo("b")]})
    out = asyncio.run(
        c.async_collect_assets("composite", '{"albums":[],"people":[],"favorites":false}')
    )
    assert [p["UID"] for p in out] == ["a", "b"]
    assert c.calls == [{}]


# ── _is_image ──────────────────────────────────────────────────────────────

def test_is_image_skips_video():
    assert photoprism._is_image(_photo("a", type_="image")) is True
    assert photoprism._is_image(_photo("a", type_="live")) is True
    assert photoprism._is_image(_photo("a", type_="video")) is False
    assert photoprism._is_image({"Type": "image"}) is False  # no UID/Hash
