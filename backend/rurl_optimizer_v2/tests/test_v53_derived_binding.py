"""V53 regression test: `derived` must be bound at function-body level.

Background
----------
`process_url_v2` assigns ``derived = derive_search_redirect(...)`` inside

    if has_matchable and parsed.main_category and parsed.keyword:

but the V53 block (~line 3550) reads it at function-body level, in its own guard:

    if (final_redirect_url and '/c/' in final_redirect_url
            and final_match_type == 'multi'
            and '[maincat]' in (final_reason or '')
            and not parsed.existing_facet
            and derived.get('dom_cat_url_slug')):   # <- unbound if the block was skipped

A row whose query is nothing but shop names and/or stopwords has
``has_matchable = False`` (see the ``non_stopword_keywords`` count) yet can still
leave the matcher as a multi-facet maincat-level ``/c/`` redirect. That row hits
V53's guard, dereferences an unbound local, and the ``UnboundLocalError``
propagates out of ``pool.imap_unordered`` — killing the WHOLE run. On 2026-08-19
that cost a three-hour 365-day run, at the very end.

The neighbouring block on line ~3398 gets this right by adding ``has_matchable``
to its own condition. V53 is instead fixed at the source: ``derived`` is
initialised to ``{}`` next to the other ``search_derived_*`` defaults, which is
also the semantically right fallback — with no search-derived result there is
nothing for V53 to prefer, so it must not fire.

This test is structural on purpose: reproducing the row needs the full pipeline
(caches, facets.csv, a live-ish matcher), while the property that actually
prevents the crash — a binding that always runs — is checkable with `ast`.

Run:  python -m pytest backend/rurl_optimizer_v2/tests/test_v53_derived_binding.py -q
"""
import ast
from pathlib import Path

_MAIN = Path(__file__).resolve().parent.parent / "main_parallel_v2.py"


def _process_url_v2() -> ast.FunctionDef:
    tree = ast.parse(_MAIN.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "process_url_v2":
            return node
    raise AssertionError("process_url_v2 not found in main_parallel_v2.py")


def _binds(stmt, name: str) -> bool:
    """True if this statement unconditionally binds `name`."""
    if isinstance(stmt, ast.Assign):
        return any(isinstance(t, ast.Name) and t.id == name for t in stmt.targets)
    if isinstance(stmt, ast.AnnAssign):
        return isinstance(stmt.target, ast.Name) and stmt.target.id == name
    return False


def test_derived_is_bound_at_function_body_level():
    fn = _process_url_v2()
    binding = next((s for s in fn.body if _binds(s, "derived")), None)
    assert binding is not None, (
        "`derived` is only bound inside a conditional. The V53 block reads it at "
        "function-body level, so a row that skips the search-derived step raises "
        "UnboundLocalError and takes the whole multiprocessing run down with it."
    )


def test_binding_precedes_every_read_of_derived():
    fn = _process_url_v2()
    binding = next(s for s in fn.body if _binds(s, "derived"))
    reads = [n.lineno for n in ast.walk(fn)
             if isinstance(n, ast.Name) and n.id == "derived"
             and isinstance(n.ctx, ast.Load)]
    assert reads, "no reads of `derived` left — did the V53 block move or go away?"
    assert binding.lineno < min(reads), (
        f"the `derived` fallback is on line {binding.lineno} but the first read is on "
        f"line {min(reads)} — the fallback has to come first to be one."
    )
