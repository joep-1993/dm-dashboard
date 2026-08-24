"""V60 (2026-08-24) — the cached facet pool was truncated to each facet's top-N.

`load_facets` reads one unfiltered /search/products call per category, and that
response caps every facet's value list at the N values with the most products
(8 for `ruimte`, 16 for `kleur`, 100 for `merk`; N is per facet and no query
param lifts it). Long-tail values therefore did not exist for the optimizer:
`opbergkast voor balkon` could never reach `ruimte~4945789` (Balkon), because
Balkon sat at rank 13 of 18 in Opbergkasten.

The second pass refetches the category with a filter on the facet — which
returns that facet's complete value list — for the pairs that look truncated.
"Looks truncated" is derived from the data: a facet's cap can only be the
highest value count it reaches anywhere, so pairs at that number are the
candidates and pairs below it already showed everything they had.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db_loader import DataLoader


def ctx(slug, facet_name, root=10, root_slug='meubilair', root_name='Meubels'):
    return {"facet_name": facet_name, "slug": slug, "root": root,
            "root_slug": root_slug, "root_name": root_name}


class Recorder:
    """Stands in for the Search API: records what was asked, answers from a map."""

    def __init__(self, answers):
        self.answers = answers          # slug -> [facet dicts]
        self.calls = []                 # (slug, facet_name, seed_value)

    def __call__(self, slug, filter_facet="", filter_value=None):
        self.calls.append((slug, filter_facet, filter_value))
        return self.answers.get(slug, [])

    @property
    def probed_slugs(self):
        return sorted(c[0] for c in self.calls)


def facet_block(fid, values):
    return [{"id": fid, "values": [{"id": vid, "facetValue": name, "count": cnt}
                                   for vid, name, cnt in values]}]


def run(pair_values, pair_ctx, facet_meta, answers):
    loader = DataLoader(use_cache=False)
    rec = Recorder(answers)
    loader._search_category_facets = rec
    rows = loader._reprobe_truncated_facet_values(pair_values, pair_ctx, facet_meta)
    return rows, rec


def test_only_pairs_at_their_facets_max_are_probed():
    """Opbergkasten shows 8 ruimte values (the cap), Tuinkasten only 3 — the
    second one already showed everything it had."""
    pair_values = {
        (1, 2901): {389392, 389393, 389394, 389395, 389396, 389397, 20080500, 4512514},
        (2, 2901): {389394, 389395, 389396},
    }
    pair_ctx = {(1, 2901): ctx('meubilair_389371_6383260', 'ruimte'),
                (2, 2901): ctx('meubilair_389371_9999999', 'ruimte')}
    answers = {'meubilair_389371_6383260': facet_block(
        2901, [(4945789, 'Balkon', 21), (389395, 'Woonkamer', 3902)])}

    rows, rec = run(pair_values, pair_ctx, {2901: 'ruimte'}, answers)

    assert rec.probed_slugs == ['meubilair_389371_6383260']
    assert [r['facet_value_id'] for r in rows] == [4945789]
    assert rows[0]['facet_value_name'] == 'Balkon'
    assert rows[0]['count'] == 21
    assert rows[0]['url'] == \
        '/products/meubilair/meubilair_389371_6383260/c/ruimte~4945789'
    assert rows[0]['category_id'] == 1 and rows[0]['facet_id'] == 2901


def test_known_values_are_not_duplicated():
    """Woonkamer comes back in the probe too — it's already a row."""
    pair_values = {(1, 2901): {389395}}
    pair_ctx = {(1, 2901): ctx('meubilair_389371_6383260', 'ruimte')}
    answers = {'meubilair_389371_6383260': facet_block(
        2901, [(389395, 'Woonkamer', 3902), (4945789, 'Balkon', 21)])}

    rows, _ = run(pair_values, pair_ctx, {2901: 'ruimte'}, answers)
    assert [r['facet_value_id'] for r in rows] == [4945789]


def test_other_facets_in_the_probe_response_are_ignored():
    """Only the filtered facet comes back complete; every other facet in that
    response is narrowed to the filtered product set and would be wrong."""
    pair_values = {(1, 2901): {389395}}
    pair_ctx = {(1, 2901): ctx('meubilair_389371_6383260', 'ruimte')}
    answers = {'meubilair_389371_6383260':
               facet_block(2901, [(4945789, 'Balkon', 21)])
               + facet_block(2906, [(389411, 'Wit', 9)])}

    rows, _ = run(pair_values, pair_ctx, {2901: 'ruimte'}, answers)
    assert [r['facet_id'] for r in rows] == [2901]


def test_brand_and_shop_are_left_truncated():
    pair_values = {(1, 1308): {104866, 114632}, (1, 1): {5, 6}}
    pair_ctx = {(1, 1308): ctx('meubilair_389371_6383260', 'merk'),
                (1, 1): ctx('meubilair_389371_6383260', 'winkel')}
    rows, rec = run(pair_values, pair_ctx, {1308: 'merk', 1: 'winkel'}, {})
    assert rec.calls == [] and rows == []


def test_a_facet_without_a_url_slug_cannot_be_addressed():
    """The first pass falls back to the response label when the taxonomy has no
    slug for the facet; `filters[<label>][0]` would be a bogus parameter."""
    pair_values = {(1, 4242): {1, 2}}
    pair_ctx = {(1, 4242): ctx('meubilair_389371_6383260', 'Soort kast')}
    rows, rec = run(pair_values, pair_ctx, {}, {})
    assert rec.calls == [] and rows == []


def test_the_seed_value_is_the_lowest_id_so_rebuilds_repeat_the_request():
    pair_values = {(1, 2901): {389395, 20080500, 389392}}
    pair_ctx = {(1, 2901): ctx('meubilair_389371_6383260', 'ruimte')}
    _, rec = run(pair_values, pair_ctx, {2901: 'ruimte'}, {})
    assert rec.calls == [('meubilair_389371_6383260', 'ruimte', 389392)]


def test_a_failing_probe_leaves_the_pair_as_it_was():
    class Boom(Recorder):
        def __call__(self, slug, filter_facet="", filter_value=None):
            raise RuntimeError('502')

    loader = DataLoader(use_cache=False)
    loader._search_category_facets = Boom({})
    rows = loader._reprobe_truncated_facet_values(
        {(1, 2901): {389395}}, {(1, 2901): ctx('x_1', 'ruimte')}, {2901: 'ruimte'})
    assert rows == []
