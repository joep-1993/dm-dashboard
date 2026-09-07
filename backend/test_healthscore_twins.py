"""De categorie- en maincat-builders van Healthscore zijn één mechaniek (2026-09-07).

`_refresh_cat_month`/`_refresh_maincat_month` en `_refresh_cat_knee`/`_refresh_maincat_knee`
stonden als vier losse functies ~400 regels uit elkaar in `healthscore_service.py`, met
bijna-identieke SQL. Dat is het TASKS-punt dat deze test begeleidt: ze delen nu één
implementatie met een scope-tabel, en dit pint vast dat de scopes in precies DRIE dingen
verschillen plus hun doeltabel.

WAAROM ZO GETOETST. De notitie bij het punt vroeg "een OLD-vs-NEW-harness met een volledige
build tegen Redshift". Dat hoeft niet: als de SQL die beide varianten UITVOEREN identiek is,
is het gedrag identiek. Deze test vervangt daarom de twee connectie-fabrieken door fakes die
elke `execute` opschrijven, en vergelijkt de opgevangen statements. Bij de refactor zelf zijn
de 22 statements van de vier functies vóór en ná byte-identiek gebleken (na
witruimte-normalisatie), params inbegrepen — geen Redshift-verbinding nodig, en daarmee ook
geen truncate op pa.hs2_* tijdens een test.

WAT DIT VANGT dat een gewone test niet vangt: iemand die één helft van een tweeling aanpast.
Dat is precies hoe die 400 regels zijn ontstaan, en de ENGINE-noot in de service beschrijft
wat het kostte — drie paden die dezelfde tabellen truncaten met verschillende vensters, en
wie het laatst draaide won, stil.
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import healthscore_service as hs


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


class _Cur:
    """Cursor die niets doet behalve opschrijven wat er langskomt."""

    def __init__(self, tag, log):
        self.tag, self.log = tag, log

    def execute(self, sql, params=None):
        self.log.append((self.tag, _norm(sql), params))

    def fetchall(self):
        # Zes kolommen: de month-builders lezen r[0..3], de knee-builders r[0..5].
        # Twee rijen, want `_refuse_empty` weigert terecht een lege uitkomst.
        return [(1, 202601, 10, 1.5, 2, 3), (2, 202602, 20, 2.5, 4, 5)]

    def fetchone(self):
        # `_guard_knee_shrink` leest hier een (mediaan, n)-paar.
        return (1, 1)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, tag, log):
        self.tag, self.log = tag, log

    def cursor(self):
        return _Cur(self.tag, self.log)

    def commit(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def capture(monkeypatch):
    """Geeft een functie die één builder draait en zijn statements teruggeeft."""
    log: list[tuple] = []
    monkeypatch.setattr(hs, "_redshift", lambda *a, **k: _Conn("redshift", log))
    monkeypatch.setattr(hs, "_postgres", lambda *a, **k: _Conn("postgres", log))
    monkeypatch.setattr(
        hs, "execute_values",
        lambda cur, sql, rows, page_size=None: log.append((cur.tag, _norm(sql), f"{len(rows)} rows")),
    )

    def run(fn, *args):
        log.clear()
        fn(*args)
        return list(log)

    return run


AS_OF = date(2026, 9, 7)

# De drie verschillen, letterlijk zoals de scope-tabel ze definieert.
DIM_CAT = "dv.deepest_subcat_id"
DIM_MAIN = "dc.main_category_id"
JOIN_MAIN = "JOIN datamart.dim_category dc ON dc.deepest_category_id = dv.deepest_subcat_id AND dc.deleted_ind = 0"
WHERE_MAIN = "AND dc.main_category_id NOT IN %(sentinels)s"


@pytest.mark.parametrize("cat_fn,main_fn,cat_table,main_table", [
    (hs._refresh_cat_month, hs._refresh_maincat_month,
     hs.CAT_MONTH_TABLE, hs.MAINCAT_MONTH_TABLE),
    (hs._refresh_cat_knee, hs._refresh_maincat_knee,
     hs.KNEE_TABLE, hs.MAINCAT_KNEE_TABLE),
])
def test_the_two_scopes_differ_only_in_the_documented_ways(
        capture, cat_fn, main_fn, cat_table, main_table):
    """Vervang in de maincat-SQL de drie verschillen door de cat-variant, en er moet
    letterlijk de cat-SQL overblijven. Dat is de hele belofte van de consolidatie."""
    a = capture(cat_fn, AS_OF)
    b = capture(main_fn, AS_OF)
    assert len(a) == len(b), "de tweelingen doen een verschillend aantal statements"

    for (tag_a, sql_a, _), (tag_b, sql_b, _) in zip(a, b):
        assert tag_a == tag_b, "dezelfde stap gaat naar een andere database"
        folded = (sql_b
                  .replace(f" {WHERE_MAIN}", "")
                  .replace(f" {JOIN_MAIN}", "")
                  .replace(DIM_MAIN, DIM_CAT)
                  .replace(main_table, cat_table))
        assert folded == sql_a, (
            "de maincat-variant verschilt in MEER dan de dimensie, de dim_category-join, "
            "het sentinel-filter en de tabelnaam:\n"
            f"  cat : {sql_a}\n  main: {folded}"
        )


@pytest.mark.parametrize("cat_fn,main_fn", [
    (hs._refresh_cat_month, hs._refresh_maincat_month),
    (hs._refresh_cat_knee, hs._refresh_maincat_knee),
])
def test_only_the_maincat_scope_carries_sentinels(capture, cat_fn, main_fn):
    """`sentinels` hoort bij de maincat-scope en nergens anders — een sentinel-parameter
    op de categoriekant zou een filter beloven dat de SQL daar niet heeft."""
    cat_params = capture(cat_fn, AS_OF)[0][2]
    main_params = capture(main_fn, AS_OF)[0][2]
    assert set(cat_params) == {"lo", "hi"}
    assert set(main_params) == {"lo", "hi", "sentinels"}
    assert main_params["sentinels"] == hs.MAINCAT_SENTINELS
    assert (cat_params["lo"], cat_params["hi"]) == (main_params["lo"], main_params["hi"])


def test_the_postgres_half_writes_to_the_scope_table_and_nothing_else(capture):
    """Elke scope raakt alleen zijn eigen tabel aan. De ENGINE-noot in de service
    beschrijft wat er gebeurde toen drie paden dezelfde tabellen truncaten."""
    for fn, own, other in (
        (hs._refresh_cat_month, hs.CAT_MONTH_TABLE, hs.MAINCAT_MONTH_TABLE),
        (hs._refresh_maincat_month, hs.MAINCAT_MONTH_TABLE, hs.CAT_MONTH_TABLE),
        (hs._refresh_cat_knee, hs.KNEE_TABLE, hs.MAINCAT_KNEE_TABLE),
        (hs._refresh_maincat_knee, hs.MAINCAT_KNEE_TABLE, hs.KNEE_TABLE),
    ):
        pg = [sql for tag, sql, _ in capture(fn, AS_OF) if tag == "postgres"]
        assert any(f"TRUNCATE {own}" == sql for sql in pg), f"{fn.__name__} truncate zijn tabel niet"
        assert not any(other in sql for sql in pg), f"{fn.__name__} raakt {other} aan"


def test_the_window_is_thirty_point_five_days_per_month(capture):
    """De 30,5 dagen staat er met opzet: `scripts/analysis/healthscore_caps.py` pint
    complete maanden, en juist dat verschil liet de twee paden andere caps schrijven."""
    lo, hi = hs._window(AS_OF, 24)
    assert (lo, hi) == (20240905, 20260907)
    # en de builders gebruiken datzelfde venster
    params = capture(hs._refresh_cat_month, AS_OF)[0][2]
    assert (params["lo"], params["hi"]) == (lo, hi)
