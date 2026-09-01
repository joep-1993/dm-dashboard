"""Paden en topic-configuratie voor de top-10 pijplijn.

Een *topic* is één categorie-run: ``top10/topics/<slug>/`` met daarin
``topic.json`` (welke categorie, welke zoektermen, welke modellen) en ``data/``.
Alle scripts lezen hun parameters hieruit, zodat een nieuwe categorie een
configuratiewijziging is en geen codewijziging.

De categorie zit als twee Search-API-parameters in de config:
``main_category`` (de root-id, bv. 12000) en ``category`` (de urlSlug, bv.
``huishoudelijke_apparatuur_19968037_23583843``). Let op: de getallen in die
slug zijn NIET de categorie-id — Airfryers is id 9005486 maar eindigt op
23583843. Laat ``resolve_category.py`` de slug ophalen; nooit zelf bouwen.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

TOP10 = Path(__file__).resolve().parents[1]
REPO = TOP10.parent
TOPICS = TOP10 / "topics"
CACHE = TOP10 / "cache"

load_dotenv(REPO / ".env", override=False)

# Modellen komen uit .env zodat wisselen geen codewijziging is. Reviews hebben
# een model met web_search-ondersteuning nodig; de ranking niet.
REVIEW_MODEL = os.getenv("TOP10_REVIEW_MODEL", "gpt-5.6-luna")
RANK_MODEL = os.getenv("TOP10_RANK_MODEL") or os.getenv("KOPTEKST_MODEL") or "gpt-5.6-luna"


def slugify(text: str) -> str:
    """ascii-fold + lowercase + niet-alfanumeriek -> '-' (zoals rank_top10)."""
    import re
    import unicodedata

    t = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", t.lower())).strip("-")


def singular(label: str) -> str:
    """'Airfryers' -> 'airfryer'. Grof maar genoeg: zoektermen die hierdoor
    krom worden ('matrassens') vallen vanzelf af op zoekvolume."""
    n = (label or "").lower().strip()
    return n[:-1] if n.endswith("s") and not n.endswith(("ss", "us", "as")) else n


class Topic:
    """Eén categorie-run, gelezen uit topics/<slug>/topic.json."""

    def __init__(self, directory: Path):
        self.dir = Path(directory)
        self.path = self.dir / "topic.json"
        if not self.path.exists():
            raise SystemExit(f"geen topic.json in {self.dir}")
        self.cfg = json.loads(self.path.read_text(encoding="utf-8"))

    # --- identiteit ---
    @property
    def slug(self) -> str:
        return self.cfg.get("slug") or self.dir.name

    @property
    def label(self) -> str:
        """Mensennaam van de categorie, bv. 'Airfryers'."""
        return self.cfg.get("label") or self.slug

    # --- categorie / search ---
    @property
    def category(self) -> dict:
        return self.cfg.get("category") or {}

    def search_params(self, **extra) -> dict:
        """Vaste Search-v2-parameters voor deze categorie."""
        cat = self.category
        params = {
            "mainCategory": str(cat["main_category"]),
            "category": cat["category"],
            "countryLanguage": self.cfg.get("country_language", "nl-nl"),
            "isBot": "false",
            "sort": self.cfg.get("sort", "popularity"),
            "sortDirection": self.cfg.get("sort_direction", "desc"),
            "limit": str(self.cfg.get("limit_per_term", 20)),
        }
        # Facetfilters uit de categorie-URL (/c/type_elek_fiets~23791934). Die
        # horen bij de scope van het topic: elke zoekopdracht blijft binnen dat
        # facet, zoals de gebruiker de categorie heeft opgegeven.
        for facet, values in (self.cfg.get("filters") or {}).items():
            for i, v in enumerate(values):
                params[f"filters[{facet}][{i}]"] = str(v)
        params.update({k: v for k, v in extra.items() if v is not None})
        return params

    @property
    def terms(self) -> list:
        return self.cfg.get("terms") or []

    # --- modellen ---
    @property
    def review_model(self) -> str:
        return (self.cfg.get("models") or {}).get("review") or REVIEW_MODEL

    @property
    def rank_model(self) -> str:
        return (self.cfg.get("models") or {}).get("rank") or RANK_MODEL

    # --- paden ---
    @property
    def data(self) -> Path:
        d = self.dir / "data"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def results(self) -> Path:
        d = self.data / "results"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def file(self, name: str) -> Path:
        return self.data / name

    def read_json(self, name: str, default=None):
        p = self.file(name)
        if not p.exists():
            if default is None:
                raise SystemExit(f"ontbreekt: {p} — draai de voorgaande stap eerst")
            return default
        return json.loads(p.read_text(encoding="utf-8"))

    def write_json(self, name: str, payload) -> Path:
        p = self.file(name)
        p.write_text(json.dumps(payload, indent=1, ensure_ascii=False), encoding="utf-8")
        return p

    def save(self) -> None:
        self.path.write_text(json.dumps(self.cfg, indent=1, ensure_ascii=False), encoding="utf-8")

    def __repr__(self) -> str:
        return f"<Topic {self.slug} ({self.label})>"


def new_topic_dir(label: str, when: date | None = None) -> Path:
    """topics/<datum>_bestof-<label>/ — aangemaakt, nog zonder topic.json."""
    d = TOPICS / f"{(when or date.today()).isoformat()}_bestof-{slugify(label)}"
    (d / "data").mkdir(parents=True, exist_ok=True)
    return d


def find_topic(name: str) -> Topic:
    """Zoek een topic op exacte mapnaam, anders op deelstring ('airfryers')."""
    if not TOPICS.exists():
        raise SystemExit("nog geen topics/ — draai eerst resolve_category.py")
    exact = TOPICS / name
    if exact.is_dir():
        return Topic(exact)
    hits = sorted(d for d in TOPICS.iterdir() if d.is_dir() and slugify(name) in d.name)
    if not hits:
        have = ", ".join(sorted(d.name for d in TOPICS.iterdir() if d.is_dir())) or "(geen)"
        raise SystemExit(f"geen topic dat matcht op '{name}'. Aanwezig: {have}")
    if len(hits) > 1:
        raise SystemExit(f"'{name}' matcht meerdere topics: {', '.join(h.name for h in hits)}")
    return Topic(hits[0])


def add_topic_arg(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--topic", required=True,
                        help="topic-mapnaam of deel daarvan, bv. 'airfryers'")
    return parser
