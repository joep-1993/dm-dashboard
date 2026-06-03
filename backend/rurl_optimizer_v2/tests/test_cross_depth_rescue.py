"""V32 regression tests: cross-depth brand/shop facet rescue in
UrlBuilder.build_multi_facet.

Background
----------
For a maincat-level R-URL like /products/mode/r/nike_nederlands_elftal_... the
matcher returns two score-100 facets that resolve to DIFFERENT subcategory
depths because FacetFilter keeps one representative row per facet value:

    fanshop "Nederlands Elftal" -> /products/mode/mode_432360_432464   (leaf)
    merk    "Nike"              -> /products/mode/mode_432360          (parent)

build_multi_facet picks the fanshop leaf as the landing category and used to
DROP Nike because its cached row pointed at the shallower parent — even though
Nike genuinely exists under the leaf (3416 products). V32 rescues such a
brand/shop facet after verifying the leaf-level URL really exists.

These tests are pure (no DB / no network): they construct the MatchResults
directly and inject a fake ``facet_url_exists`` set.

Run:  python -m pytest backend/rurl_optimizer_v2/tests/test_cross_depth_rescue.py -q
"""
import sys
from pathlib import Path

# Make the optimizer package importable when run from anywhere.
_V2_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_V2_ROOT))

from src.facet_filter import FacetValue
from src.matcher import MatchResult
from src.parser import ParsedRUrl
from src.url_builder import UrlBuilder


# ── Fixtures mirroring the real Nike / Nederlands Elftal case ────────────────
LEAF = "/products/mode/mode_432360_432464"          # fanshop's subcat (leaf)
PARENT = "/products/mode/mode_432360"               # merk's cached subcat (parent)

FANSHOP_FV = FacetValue(
    facet_id=3515, facet_name="fanshop", facet_value_id=1335065,
    facet_value_name="Nederlands Elftal",
    url=f"{LEAF}/c/fanshop~1335065", count=393,
)
# merk row's cached URL sits at the PARENT, one level shallower than fanshop.
MERK_FV = FacetValue(
    facet_id=48, facet_name="merk", facet_value_id=84748,
    facet_value_name="Nike",
    url=f"{PARENT}/c/merk~84748", count=3416,
)


def _mr(fv, score=100):
    return MatchResult(
        keyword=fv.facet_value_name.split()[0].lower(),
        facet_value=fv, match_type="exact", score=score,
        matched_text=fv.facet_value_name,
    )


def _maincat_rurl():
    """A maincat-level R-URL: no subcategory pinned (subcategory_name == 'mode')."""
    return ParsedRUrl(
        original_url="https://www.beslist.nl/products/mode/r/"
                     "nike_nederlands_elftal_uitshirt_2020-2022/",
        category_path="mode",
        full_category_path="/products/mode",
        main_category="mode",
        subcategory_id="",
        subcategory_name="mode",
        keyword="nike nederlands elftal uitshirt 2020-2022",
    )


def _builder_with_leaf_brand(brand_exists_in_leaf=True):
    """UrlBuilder whose facet_url_exists knows the leaf-level merk URL."""
    known = {
        f"{LEAF}/c/fanshop~1335065",
        f"{PARENT}/c/merk~84748",
    }
    if brand_exists_in_leaf:
        known.add(f"{LEAF}/c/merk~84748")   # Nike genuinely lives under the leaf
    b = UrlBuilder()
    b.facet_url_exists = known.__contains__
    return b


# ── Tests ────────────────────────────────────────────────────────────────────
def test_brand_rescued_to_primary_leaf_subcat():
    """The Nike merk facet, parked at the parent, is appended to the fanshop
    leaf once its leaf-level URL is verified to exist."""
    b = _builder_with_leaf_brand(brand_exists_in_leaf=True)
    res = b.build_multi_facet(_maincat_rurl(), [_mr(FANSHOP_FV), _mr(MERK_FV)])

    assert res.success
    assert res.redirect_url == f"https://www.beslist.nl{LEAF}/c/fanshop~1335065~~merk~84748"
    assert res.facet_count == 2
    assert "merk" in res.facet_names and "fanshop" in res.facet_names
    assert "Nike" in res.facet_value_names


def test_brand_not_rescued_when_absent_from_leaf():
    """If the brand does NOT exist under the primary's leaf, it is still
    dropped — we must never fabricate a dead-end facet URL."""
    b = _builder_with_leaf_brand(brand_exists_in_leaf=False)
    res = b.build_multi_facet(_maincat_rurl(), [_mr(FANSHOP_FV), _mr(MERK_FV)])

    assert res.success
    assert res.redirect_url == f"https://www.beslist.nl{LEAF}/c/fanshop~1335065"
    assert res.facet_count == 1
    assert "merk" not in res.facet_names


def test_no_checker_keeps_legacy_drop_behaviour():
    """When facet_url_exists is unset (standalone callers / old wiring), the
    cross-depth facet is dropped exactly as before — no rescue, no crash."""
    b = UrlBuilder()  # facet_url_exists left as None
    res = b.build_multi_facet(_maincat_rurl(), [_mr(FANSHOP_FV), _mr(MERK_FV)])

    assert res.success
    assert res.redirect_url == f"https://www.beslist.nl{LEAF}/c/fanshop~1335065"
    assert res.facet_count == 1


def test_rescue_limited_to_merk_and_winkel_axes():
    """A non-brand/shop facet (e.g. a type facet) parked at a different depth
    is NOT rescued even if its leaf URL exists — only merk/winkel qualify."""
    type_fv = FacetValue(
        facet_id=999, facet_name="type_mode", facet_value_id=555,
        facet_value_name="Uitshirts", url=f"{PARENT}/c/type_mode~555", count=10,
    )
    known = {
        f"{LEAF}/c/fanshop~1335065",
        f"{PARENT}/c/type_mode~555",
        f"{LEAF}/c/type_mode~555",   # exists in leaf, but type axis is not eligible
    }
    b = UrlBuilder()
    b.facet_url_exists = known.__contains__
    res = b.build_multi_facet(_maincat_rurl(), [_mr(FANSHOP_FV), _mr(type_fv)])

    assert res.redirect_url == f"https://www.beslist.nl{LEAF}/c/fanshop~1335065"
    assert res.facet_count == 1
    assert "type_mode" not in res.facet_names


if __name__ == "__main__":
    # Allow running without pytest:  python tests/test_cross_depth_rescue.py
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
