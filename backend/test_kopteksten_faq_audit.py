"""
Regression tests for the Phase-0 Kopteksten/FAQ audit fixes (2026-06-12).

Covers the four HIGH findings:
  #1 batch_api_service: kopteksten failed-url insert passed `failed_urls`
     (url strings) instead of `rows` (url_ids) -> aborted the whole commit.
  #2 batch_api_service: kopteksten result loop lacked the blank-line guard and
     referenced an unbound `result` in its except handler.
  #3 main.py: DELETE /api/faq/result was registered twice; the live copy did
     not clear pa.url_validation, so a skipped->deleted URL never returned to
     FAQ pending.
  #4 link_validator: verdict map keyed by lookup_value alone -> the same
     pimId/plpUrl under two different maincats clobbered each other, making
     dead/alive verdicts depend on thread-completion order.

#4 is a true behavioural unit test (ES + maincat mapping monkeypatched).
#1/#2/#3 are exercised as source-level guards (the surrounding code needs a
live OpenAI Batch run / DB, so we assert the bug patterns are gone).

Run:  ./venv/bin/python -m pytest backend/test_phase0_fixes.py -q
   or ./venv/bin/python backend/test_phase0_fixes.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


# --------------------------------------------------------------------------
# #4 — link_validator cross-maincat collision (behavioural)
# --------------------------------------------------------------------------
def _patch_link_validator(monkeyish, es_by_index, content_links):
    """Stub out the network/IO bits of link_validator so the keying logic
    can be exercised deterministically. `monkeyish` is a list we append undo
    callables to. es_by_index maps index-name -> {pimId: plpUrl_or_None}."""
    import backend.link_validator as lv

    saved = {
        "extract_hyperlinks_from_content": lv.extract_hyperlinks_from_content,
        "load_maincat_mapping": lv.load_maincat_mapping,
        "query_elasticsearch": lv.query_elasticsearch,
        "extract_from_url": lv.extract_from_url,
    }
    for name, fn in saved.items():
        monkeyish.append((name, fn))

    lv.extract_hyperlinks_from_content = lambda content: list(content_links.keys())
    lv.load_maincat_mapping = lambda: {}
    # content_links maps url -> (maincat_id, pim_id). is_v4 always False here.
    lv.extract_from_url = lambda url, mapping: (
        content_links[url][0], content_links[url][1], False
    ) if url in content_links else (None, None, False)
    lv.query_elasticsearch = lambda index, pim_ids, min_offers=2: {
        pid: es_by_index.get(index, {}).get(pid) for pid in pim_ids
        if pid in es_by_index.get(index, {})
    }
    return lv


def _restore(lv, monkeyish):
    for name, fn in monkeyish:
        setattr(lv, name, fn)


def test_cross_maincat_pimid_no_collision():
    """Same pimId under two maincats: one alive, one gone. Before the fix the
    shared lookup_value key let one verdict overwrite the other. After the fix
    each URL gets its own (maincat, pimId)-keyed verdict."""
    INDEX_PREFIX = __import__("backend.link_validator", fromlist=["INDEX_PREFIX"]).INDEX_PREFIX
    shared_pim = "nl-nl-gold-12345"
    url_alive = "/p/elektronica/product-a/12345"
    url_gone = "/p/wonen/product-b/12345"
    content_links = {
        url_alive: ("19875530", shared_pim),   # elektronica -> alive
        url_gone:  ("19968036", shared_pim),    # wonen -> gone (None)
    }
    es_by_index = {
        f"{INDEX_PREFIX}19875530": {shared_pim: "https://www.beslist.nl/p/elektronica/product-a/12345"},
        f"{INDEX_PREFIX}19968036": {shared_pim: None},
    }

    undo = []
    lv = _patch_link_validator(undo, es_by_index, content_links)
    try:
        result, unknown = lv.lookup_plp_urls_for_content("dummy")
    finally:
        _restore(lv, undo)

    assert unknown == [], f"unexpected unknown-format links: {unknown}"
    assert result[url_alive] is not None, "alive URL wrongly classified gone"
    assert result[url_gone] is None, "gone URL wrongly classified alive (collision!)"


def test_faq_cross_maincat_pimid_no_collision():
    """Same as above but through the FAQ validation path (validate_faq_links)."""
    import json
    INDEX_PREFIX = __import__("backend.link_validator", fromlist=["INDEX_PREFIX"]).INDEX_PREFIX
    shared_pim = "nl-nl-gold-99999"
    url_alive = "https://www.beslist.nl/p/elektronica/x/99999"
    url_gone = "https://www.beslist.nl/p/wonen/y/99999"
    content_links = {
        url_alive: ("19875530", shared_pim),
        url_gone:  ("19968036", shared_pim),
    }
    es_by_index = {
        f"{INDEX_PREFIX}19875530": {shared_pim: "https://www.beslist.nl/p/elektronica/x/99999"},
        f"{INDEX_PREFIX}19968036": {shared_pim: None},
    }

    import backend.link_validator as lv
    undo = [("extract_hyperlinks_from_faq_json", lv.extract_hyperlinks_from_faq_json)]
    saved = {
        "load_maincat_mapping": lv.load_maincat_mapping,
        "query_elasticsearch": lv.query_elasticsearch,
        "extract_from_url": lv.extract_from_url,
    }
    for n, f in saved.items():
        undo.append((n, f))
    lv.extract_hyperlinks_from_faq_json = lambda faq_json: list(content_links.keys())
    lv.load_maincat_mapping = lambda: {}

    def _ef(url, mapping):
        # validate_faq_links strips the https://www.beslist.nl prefix before
        # calling extract_from_url, so match both absolute and relative forms.
        for k, (mc, pim) in content_links.items():
            if url in (k, k.replace("https://www.beslist.nl", "")):
                return (mc, pim, False)
        return (None, None, False)
    lv.extract_from_url = _ef
    lv.query_elasticsearch = lambda index, pim_ids, min_offers=2: {
        pid: es_by_index.get(index, {}).get(pid) for pid in pim_ids
        if pid in es_by_index.get(index, {})
    }
    try:
        res = lv.validate_faq_links(json.dumps({"faqs": []}))
    finally:
        _restore(lv, undo)

    assert url_gone in res["gone_links"], "gone URL not detected (collision!)"
    assert url_alive not in res["gone_links"], "alive URL wrongly marked gone"
    assert res["valid_links"] == 1


# --------------------------------------------------------------------------
# #1/#2/#3 — source-level guards
# --------------------------------------------------------------------------
def _read(rel):
    return open(os.path.join(ROOT, rel), encoding="utf-8").read()


def test_kopteksten_failed_insert_uses_rows_not_failed_urls():
    """#1: the kopteksten failed-url INSERT must pass `rows`, never the raw
    `failed_urls` (url strings would violate the BIGINT url_id column)."""
    src = _read("backend/batch_api_service.py")
    # The kopteksten block builds `rows` then must execute_batch with `rows`.
    kopt = src[src.index("INSERT INTO pa.kopteksten_jobs"):]
    snippet = kopt[: kopt.index(")") + 1500]
    assert "updated_at = CURRENT_TIMESTAMP" in snippet
    # The exact regression: `failed_urls\n  )` as the execute_batch argument.
    assert not re.search(r'CURRENT_TIMESTAMP",\s*\n\s*failed_urls\s*\n\s*\)', kopt), \
        "kopteksten insert still passes failed_urls instead of rows (#1 regressed)"


def test_kopteksten_result_loop_has_blank_line_guard():
    """#2: the kopteksten batch-result loop must skip blank lines and must not
    read an unbound `result` in its except handler."""
    src = _read("backend/batch_api_service.py")
    marker = "for line in results_text.strip().split('\\n'):"
    # Two such loops exist (FAQ then kopteksten). The kopteksten one is last.
    assert src.count(marker) == 2, "expected exactly 2 batch-result loops (FAQ + kopteksten)"
    loop_start = src.rindex(marker)
    block = src[loop_start: loop_start + 1200]
    assert "if not line.strip():" in block, "kopteksten loop missing blank-line guard (#2 regressed)"
    assert "isinstance(result, dict)" in block, "kopteksten except still trusts unbound result (#2 regressed)"


def test_faq_delete_route_single_and_clears_url_validation():
    """#3: exactly one DELETE /api/faq/result route, and it clears url_validation."""
    src = _read("backend/main.py")
    n = src.count('@app.delete("/api/faq/result/{url:path}")')
    assert n == 1, f"expected 1 faq delete route, found {n} (#3 regressed)"
    # Locate the live handler body and confirm it deletes from url_validation.
    h = src.index("async def delete_faq_result")
    body = src[h: h + 1400]
    assert "DELETE FROM pa.url_validation" in body, \
        "live faq delete no longer clears url_validation (#3 regressed)"
    assert "DELETE FROM pa.faq_link_validation" in body


# --------------------------------------------------------------------------
# Phase 1
# --------------------------------------------------------------------------
_SAMPLE_PAGE = {
    "h1_title": "Vloeibare wasmiddelen",
    "url": "/products/huishoudelijke_apparatuur/x/c/merk~123",
    "products": [{"title": "Ariel Vloeibaar", "description": "1.5L fles"}],
    "product_urls": [{"label": "Ariel", "url": "https://www.beslist.nl/p/ariel/6/123/"}],
    "selected_facets": [{"facet_name": "Merk", "facet_value": "Ariel"}],
}


def test_faq_prompt_builders_identical():
    """#5: the batch and real-time FAQ prompt must come from one builder, so
    build_faq_prompt == batch._build_faq_prompt for identical input."""
    from backend.faq_service import build_faq_prompt
    from backend.batch_api_service import _build_faq_prompt
    a = build_faq_prompt(_SAMPLE_PAGE, 6)
    b = _build_faq_prompt(_SAMPLE_PAGE, 6)
    assert a == b, "batch and real-time FAQ prompts diverged (#5 regressed)"
    # canonical (richer) markers that the batch copy used to be missing
    assert "ook niet als onderdeel van een langere linktekst" in a
    assert "Beeztees kattentuigje Hearts" in a
    assert "/p/productnaam/category_id/pim_id/" in a


def test_num_faqs_defaults_are_six():
    """#5: every FAQ entrypoint defaults num_faqs to 6 (was 5 in two of them)."""
    import inspect
    from backend import faq_service, batch_api_service
    for fn in (faq_service.build_faq_prompt, faq_service.generate_faqs_for_page,
               faq_service.process_single_url_faq, batch_api_service._build_faq_prompt):
        default = inspect.signature(fn).parameters["num_faqs"].default
        assert default == 6, f"{fn.__name__} num_faqs default is {default}, expected 6 (#5 regressed)"


def test_http_exception_not_swallowed():
    """#6: process_urls/upload/validate_links re-raise HTTPException before the
    broad handler so deliberate 4xx aren't remapped to 500."""
    src = _read("backend/main.py")
    # Each broad 500 handler must be immediately preceded by an HTTPException re-raise.
    guard = "except HTTPException:\n        raise"
    assert src.count(guard) >= 3, "missing 'except HTTPException: raise' guards (#6 regressed)"


def test_kopteksten_delete_backs_up_to_history():
    """#9: the manual kopteksten delete archives to content_history before DELETE."""
    src = _read("backend/main.py")
    h = src.index("async def delete_result")
    body = src[h: h + 2600]
    assert "INSERT INTO pa.content_history" in body, "manual delete no longer backs up (#9 regressed)"
    assert "'manual_delete'" in body
    # backup must come before the content delete
    assert body.index("INSERT INTO pa.content_history") < body.index("DELETE FROM pa.kopteksten_content")


# --------------------------------------------------------------------------
# Phase 2 (behaviour-preserving cleanup)
# --------------------------------------------------------------------------
def test_batch_fetchers_ordered_and_log_truncation():
    """#7: both pending-fetchers ORDER BY url_id (deterministic backlog) and log a hit cap."""
    src = _read("backend/batch_api_service.py")
    assert src.count("ORDER BY u.url_id") >= 2, "batch fetchers missing ORDER BY (#7 regressed)"
    assert src.count("deferred to next run") >= 2, "missing fetch-cap truncation log (#7 regressed)"


def test_poll_loops_have_timeout_ceiling():
    """#8: both batch poll loops have a wall-clock ceiling so a stuck batch can't spin forever."""
    src = _read("backend/batch_api_service.py")
    assert src.count("poll_started = time.monotonic()") == 2, "poll loops missing timeout start (#8 regressed)"
    assert src.count("MAX_POLL_SECONDS = 26 * 3600") == 2, "poll loops missing timeout ceiling (#8 regressed)"


def test_failure_reasons_endpoint_removed():
    """#10: the dead /api/failure-reasons endpoint is gone."""
    src = _read("backend/main.py")
    assert "/api/failure-reasons" not in src, "dead /api/failure-reasons endpoint still present (#10 regressed)"


def test_dead_link_validator_functions_removed_live_kept():
    """#11: the 4 dead functions are deleted; the live ones remain."""
    src = _read("backend/link_validator.py")
    for name in ("update_content_in_redshift", "add_urls_to_werkvoorraad",
                 "validate_and_fix_content_batch", "validate_faq_batch"):
        assert f"def {name}" not in src, f"{name} should be deleted (#11 regressed)"
    for name in ("validate_and_fix_content_links", "validate_content_links",
                 "validate_faq_links", "lookup_plp_urls_for_content",
                 "mark_faq_failed_unknown_format"):
        assert f"def {name}" in src, f"live function {name} was wrongly removed (#11)"


def test_faq_json_to_html_robust_guard():
    """#12: faq_json_to_html guards non-list payloads and catches AttributeError,
    without escaping (answers legitimately carry <a> HTML)."""
    src = _read("backend/main.py")
    h = src.index("def faq_json_to_html")
    body = src[h: h + 800]
    assert "isinstance(faqs, list)" in body, "missing list-shape guard (#12 regressed)"
    assert "AttributeError" in body, "AttributeError not caught (#12 regressed)"


# --------------------------------------------------------------------------
# Phase 3 (de-dup: shared _lookup_links core)
# --------------------------------------------------------------------------
def test_lookup_helpers_delegate_to_shared_core():
    """Both link-validation entrypoints must route through _lookup_links so they
    can't silently re-fork (and a shared helper must exist)."""
    src = _read("backend/link_validator.py")
    assert "def _lookup_links(" in src, "shared _lookup_links helper missing"
    for fn in ("lookup_plp_urls_for_content", "validate_faq_links"):
        h = src.index(f"def {fn}(")
        nxt = src.index("\ndef ", h + 1)
        body = src[h:nxt]
        assert "_lookup_links(links)" in body, f"{fn} no longer delegates to _lookup_links"


def test_faq_validate_counts_valid_gone_unknown():
    """Behavioural: through the shared core, validate_faq_links counts a valid,
    a gone, and an unknown-format link correctly (locks total_links == distinct
    links, the old len(set(links)))."""
    import json
    INDEX_PREFIX = __import__("backend.link_validator", fromlist=["INDEX_PREFIX"]).INDEX_PREFIX
    url_valid = "/p/elektronica/a/111"
    url_gone = "/p/elektronica/b/222"
    url_unknown = "/p/some-new-format/weird"
    content_links = {
        url_valid: ("19875530", "nl-nl-gold-111"),
        url_gone:  ("19875530", "nl-nl-gold-222"),
        # url_unknown deliberately absent -> extract_from_url returns None
    }
    es = {f"{INDEX_PREFIX}19875530": {"nl-nl-gold-111": "https://www.beslist.nl/p/elektronica/a/111",
                                      "nl-nl-gold-222": None}}

    import backend.link_validator as lv
    undo = [(n, getattr(lv, n)) for n in
            ("extract_hyperlinks_from_faq_json", "load_maincat_mapping",
             "query_elasticsearch", "extract_from_url")]
    lv.extract_hyperlinks_from_faq_json = lambda faq_json: [url_valid, url_gone, url_unknown]
    lv.load_maincat_mapping = lambda: {}
    lv.extract_from_url = lambda url, mapping: (
        (*content_links[url], False) if url in content_links else (None, None, False)
    )
    lv.query_elasticsearch = lambda index, pim_ids, min_offers=2: {
        pid: es.get(index, {}).get(pid) for pid in pim_ids if pid in es.get(index, {})
    }
    try:
        res = lv.validate_faq_links(json.dumps({"faqs": []}))
    finally:
        _restore(lv, undo)

    assert res["total_links"] == 3, res
    assert res["valid_links"] == 1, res
    assert res["gone_links"] == [url_gone], res
    assert res["unknown_format_links"] == [url_unknown], res


def test_validate_results_apply_helper_shared():
    """Both validate paths route the destructive gone-products reset through one
    shared helper, so it lives in exactly one place."""
    src = _read("backend/main.py")
    assert "def _apply_corrected_and_gone(" in src, "shared validate-apply helper missing"
    # 1 def + 2 call sites
    assert src.count('_apply_corrected_and_gone(') == 3, "both validate paths must call the shared helper"
    assert 'log_prefix="VALIDATE-LINKS"' in src and 'log_prefix="VALIDATE-ALL"' in src, \
        "both validate paths must delegate (sync + background)"
    # the IN-list content delete (gone-products reset) must exist only inside the helper
    assert src.count("DELETE FROM pa.kopteksten_content WHERE url_id IN") == 1, \
        "gone-products content delete is duplicated outside the helper (drift risk)"


# --------------------------------------------------------------------------
# Phase 4 (remaining MED correctness)
# --------------------------------------------------------------------------
def test_faq_sync_validate_records_all_urls():
    """The sync FAQ validate-links must record ALL urls with the real gone
    count (not only clean ones, not hardcoded 0) — else gone/unknown URLs get
    re-fetched forever."""
    src = _read("backend/main.py")
    # the old bug: insert with a literal 0 gone_links for the clean-only branch
    assert "record['valid_links'], 0))" not in src, \
        "sync FAQ validate-links still hardcodes gone_links=0 (P4 regressed)"
    # both sync and background now compute gone_count for the faq insert
    assert src.count("record['valid_links'], gone_count)") >= 1


def test_faq_url_validation_is_canonical_not_substring():
    """FAQ link cleaning must use canonical equality, not substring containment."""
    src = _read("backend/faq_service.py")
    assert "valid_url in url or url in valid_url" not in src, \
        "substring URL validation still present (P4 regressed)"
    assert "url_norm == valid_url.rstrip" in src, "canonical URL comparison missing (P4 regressed)"


def test_delete_endpoints_report_not_found_and_canonical():
    """Both delete endpoints report found/not_found and the kopteksten one resets
    Redshift on the canonical URL after the local delete (not the raw url first)."""
    src = _read("backend/main.py")
    assert src.count('"status": "not_found"') >= 2, "delete endpoints must report not_found (P4 regressed)"
    # delete_result: redshift reset matched on canon, not raw url
    h = src.index("async def delete_result")
    body = src[h: src.index("\n@app", h + 1)]
    assert "SET kopteksten = 0" in body and "(canon,)" in body, \
        "Redshift flag reset must match on canon (P4 regressed)"
    assert "(url,)" not in body, "Redshift reset still matches raw url (P4 regressed)"
    # local delete happens before the redshift reset
    assert body.index("DELETE FROM pa.kopteksten_content") < body.index("def reset_redshift_flag")


# --------------------------------------------------------------------------
# Phase 5 cherry-picks (anti-drift + latent correctness)
# --------------------------------------------------------------------------
def test_url_helpers_single_sourced():
    """parse_beslist_url / build_api_params / clean_url are one shared object,
    not duplicated copies that can drift."""
    import backend.faq_service as f
    import backend.scraper_service as s
    assert f.parse_beslist_url is s.parse_beslist_url, "parse_beslist_url duplicated (drift risk)"
    assert f.build_api_params is s.build_api_params, "build_api_params duplicated"
    assert f.clean_url is s.clean_url, "clean_url duplicated"
    # still functional after the move
    assert f.parse_beslist_url("/products/klussen/klussen_1_2/c/merk~123") == \
        ("klussen", "klussen_1_2", {"merk": ["123"]})
    # the faq_service source must no longer redefine them
    src = _read("backend/faq_service.py")
    assert "def parse_beslist_url(" not in src, "faq_service still redefines parse_beslist_url"
    assert "def build_api_params(" not in src, "faq_service still redefines build_api_params"


def test_replace_url_in_content_normalized_match():
    """replace_url_in_content matches hrefs on a normalized form, so a corrected
    URL is written even when the stored href differs cosmetically (abs/rel,
    trailing slash) from the lookup form."""
    import backend.link_validator as lv
    # absolute href in content, relative old_url
    out = lv.replace_url_in_content(
        '<a href="https://www.beslist.nl/p/x/6/111">X</a>', "/p/x/6/111", "/p/y/6/222")
    assert "/p/y/6/222" in out and "/p/x/6/111" not in out, out
    # trailing-slash mismatch
    out2 = lv.replace_url_in_content('<a href="/p/x/6/111">X</a>', "/p/x/6/111/", "/p/y/6/222")
    assert "/p/y/6/222" in out2, out2
    # genuinely different URL is left untouched
    out3 = lv.replace_url_in_content('<a href="/p/x/6/111">X</a>', "/p/z/6/999", "/p/y/6/222")
    assert "/p/x/6/111" in out3 and "/p/y/6/222" not in out3, out3


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
