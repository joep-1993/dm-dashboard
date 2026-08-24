"""V61 — _subcat_keyword_facet and _fetch_subcat_facets built the same request.

_do_probe_inner asked for a subcat-level facet lookup and _do_probe then fetched
the very same URL again for the multi-facet/size extractors, so every pair with a
leftover query token cost two calls against the token bucket that is the run's
bottleneck. The parsing is unchanged; only the fetch is shared.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.facet_probe as fp


FACETS = [
    {"urlName": "winkel", "values": [{"id": 1, "facetValue": "Kanten winkel", "count": 99}]},
    {"urlName": "type_blouse", "values": [
        {"id": 10, "facetValue": "Kanten blouse", "count": 12},
        {"id": 11, "facetValue": "Zijden blouse", "count": 40},
        {"id": 12, "facetValue": "Kanten blouse", "count": 3},
    ]},
    {"urlName": "kleur", "values": [{"id": 20, "facetValue": "Wit", "count": 7},
                                    {"id": 21, "facetValue": "Zwart", "count": 0}]},
]


def test_parses_the_passed_facets_without_fetching(monkeypatch):
    calls = []
    monkeypatch.setattr(fp, "_fetch_subcat_facets",
                        lambda *a, **k: calls.append(a) or [])
    hit = fp._subcat_keyword_facet("mode_1", "wit kanten blouse", None, facets=FACETS)
    assert calls == [], "een meegegeven facetlijst mag geen call kosten"
    # winkel is uitgesloten; van de twee 'Kanten blouse'-waarden wint de hoogste count
    assert hit == ("type_blouse", 10, "Kanten blouse", 12)


def test_falls_back_to_fetching_when_nothing_is_passed(monkeypatch):
    calls = []

    def _fake(slug, kw, bucket):
        calls.append((slug, kw))
        return FACETS

    monkeypatch.setattr(fp, "_fetch_subcat_facets", _fake)
    hit = fp._subcat_keyword_facet("mode_1", "wit kanten blouse", None)
    assert calls == [("mode_1", "wit kanten blouse")]
    assert hit == ("type_blouse", 10, "Kanten blouse", 12)


def test_a_failed_fetch_is_not_a_match(monkeypatch):
    monkeypatch.setattr(fp, "_fetch_subcat_facets", lambda *a, **k: None)
    assert fp._subcat_keyword_facet("mode_1", "wit kanten blouse", None) is None


def test_zero_count_and_blacklisted_axes_are_skipped():
    only_zero = [{"urlName": "kleur", "values": [{"id": 21, "facetValue": "Wit", "count": 0}]}]
    assert fp._subcat_keyword_facet("mode_1", "wit", None, facets=only_zero) is None
    only_shop = [{"urlName": "winkel", "values": [{"id": 1, "facetValue": "Wit", "count": 5}]}]
    assert fp._subcat_keyword_facet("mode_1", "wit", None, facets=only_shop) is None


def test_do_probe_reuses_the_list_do_probe_inner_fetched(monkeypatch):
    """The whole point: one fetch, not two."""
    fetches = []
    monkeypatch.setattr(fp, "_fetch_subcat_facets",
                        lambda slug, kw, bucket: fetches.append((slug, kw)) or FACETS)
    monkeypatch.setattr(fp, "_probe_put", lambda *a, **k: None)
    v28 = {"dom_cat_url_slug": "mode_1", "dom_cat_name": "Blouses",
           "dom_cat_count": 100, "mode": "and"}

    class _Bucket:
        def acquire(self):
            pass

    fp._do_probe("mode", "wit kanten blouse", v28, _Bucket())
    assert len(fetches) == 1, f"verwachtte 1 subcat-fetch, kreeg {len(fetches)}: {fetches}"
