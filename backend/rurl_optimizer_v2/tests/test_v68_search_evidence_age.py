"""Het search-bewijs draagt zijn eigen ophaaldatum mee.

De cache heeft een TTL van 7 dagen, dus twee runs over dezelfde URL kunnen op
ander bewijs staan zonder dat er iets aan de code of de site veranderde. Op
02-09 kostte dat een halve diagnose: `dom_cat_share` ging van 0,84 naar 0,37
puur doordat de cache verliep. Het tijdstip stond alleen in een kolom van de
sqlite; nu reist het mee in de payload, zodat de reviewsheet het kan noemen.
"""
import os, sys
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from src import search_derived as sd


def _temp_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(sd, '_CACHE_DB_PATH', tmp_path / 'search_derived.sqlite')


def test_cache_get_stamps_the_fetch_time(tmp_path, monkeypatch):
    _temp_cache(tmp_path, monkeypatch)
    mn, kn = sd._normalize('Tuin', 'tuinkast kunststof')
    sd._cache_put(mn, kn, {'schema_version': sd.SCHEMA_VERSION, 'mode': 'and',
                           'total': 42, 'dom_cat_name': 'Tuinkasten',
                           'dom_cat_share': 0.84})
    got = sd._cache_get(mn, kn)
    assert got is not None
    assert got['fetched_at'], "de payload moet zeggen wanneer hij opgehaald is"
    assert got['dom_cat_share'] == 0.84  # en de rest blijft ongemoeid


def test_the_stamp_survives_derive_redirect(tmp_path, monkeypatch):
    """derive_redirect is wat de workers aanroepen — daar moet het uitkomen."""
    _temp_cache(tmp_path, monkeypatch)
    mn, kn = sd._normalize('Tuin', 'tuinkast kunststof')
    sd._cache_put(mn, kn, {'schema_version': sd.SCHEMA_VERSION, 'mode': 'and',
                           'total': 42, 'dom_cat_name': 'Tuinkasten',
                           'dom_cat_share': 0.84})
    assert sd.derive_redirect('Tuin', 'tuinkast kunststof')['fetched_at']


def test_a_miss_has_nothing_to_date(tmp_path, monkeypatch):
    _temp_cache(tmp_path, monkeypatch)
    out = sd.derive_redirect('Tuin', 'nooit opgehaald')
    assert out['mode'] == 'uncached'
    assert not out.get('fetched_at')
