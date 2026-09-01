"""Eén web-search-review ophalen bij OpenAI, met bronnen en kosten.

Dit is de module die de geïmporteerde skill wél aanriep maar niet meebracht.
Hij draait op de Responses API, want alleen die heeft de ingebouwde
``web_search``-tool; ``chat.completions`` (de rest van deze repo) kan dat niet.

Teruggegeven dict is het contract dat ``run_reviews.py`` en
``export_top10_data.py`` verwachten. ``error`` is None bij succes; bij een
mislukking staat daar de melding en blijft de rest leeg, zodat een kapotte
review nooit als geldig resultaat wordt gecachet.

Kosten worden alleen berekend voor modellen die in ``pricing.json`` staan.
Staat een model daar niet in, dan blijft ``cost_usd.total`` None — een geraden
tarief is erger dan geen tarief.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from .topic import REPO, REVIEW_MODEL

PRICING_PATH = Path(__file__).with_name("pricing.json")
_warned: set[str] = set()
_client: OpenAI | None = None


def client() -> OpenAI:
    global _client
    if _client is None:
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            raise SystemExit(f"OPENAI_API_KEY ontbreekt (verwacht in {REPO / '.env'})")
        _client = OpenAI(api_key=key)
    return _client


def _pricing() -> dict:
    if not PRICING_PATH.exists():
        return {}
    return json.loads(PRICING_PATH.read_text(encoding="utf-8"))


def cost_for(model: str, usage: dict, n_searches: int = 0) -> dict:
    """{'input':…, 'output':…, 'web_search':…, 'total':…} of None-waarden."""
    rates = _pricing().get(model)
    if not rates:
        if model not in _warned:
            _warned.add(model)
            print(f"[pricing] geen tarief voor '{model}' in {PRICING_PATH.name} — kosten blijven None")
        return {"input": None, "output": None, "web_search": None, "total": None}
    # Gecachete input is een orde goedkoper ($0,02 vs $0,20 per 1M bij Luna) en
    # zit al ín input_tokens, dus die eerst eraf halen voordat je het volle
    # tarief rekent — anders overschat je elke herhaalde prompt.
    total_in = usage.get("input_tokens", 0)
    cached = usage.get("cached_tokens", 0) if "cached_input" in rates else 0
    inp = (total_in - cached) / 1e6 * rates["input"] + cached / 1e6 * rates.get("cached_input", 0.0)
    out = usage.get("output_tokens", 0) / 1e6 * rates["output"]
    ws = n_searches * rates.get("web_search_call", 0.0)
    return {"input": round(inp, 6), "output": round(out, 6), "web_search": round(ws, 6),
            "total": round(inp + out + ws, 6)}


def _extract(payload: dict) -> dict:
    """Haal tekst, zoekopdrachten en bron-URL's uit een Responses-payload.

    Defensief op dict-niveau in plaats van via attributen: de vorm van
    ``output``-items verschilt per modelfamilie en per SDK-versie, en een
    AttributeError midden in een betaalde run is zonde van de call.
    """
    text_parts, queries, citations, opened = [], [], [], []
    for item in payload.get("output") or []:
        itype = item.get("type")
        if itype == "web_search_call":
            # Twee smaken: een zoekopdracht ('search', met query/queries) en het
            # daadwerkelijk openen van een pagina ('open_page', met url). Dat
            # tweede is de sterkste bron: die pagina heeft het model echt gezien.
            action = item.get("action") or {}
            for q in ([action.get("query")] if action.get("query") else []) + list(action.get("queries") or []):
                if q and q not in queries:
                    queries.append(q)
            if action.get("url"):
                opened.append(action["url"])
        elif itype == "message":
            for block in item.get("content") or []:
                if block.get("type") in ("output_text", "text"):
                    text_parts.append(block.get("text") or "")
                for ann in block.get("annotations") or []:
                    if ann.get("type") == "url_citation" and ann.get("url"):
                        citations.append({"url": ann["url"], "title": ann.get("title")})
    seen, unique = set(), []
    for c in citations:
        if c["url"] not in seen:
            seen.add(c["url"])
            unique.append(c)
    return {"text": "\n".join(t for t in text_parts if t), "queries": queries,
            "citations": unique, "opened": list(dict.fromkeys(opened))}


def _check_urls(markdown: str, citations: list[dict], opened: list[str] | None = None):
    """Welke URL's uit de tekst zijn niet terug te voeren op een echte zoekhit?

    Geeft (alles_herleidbaar, niet-herleidbare URL's). Een niet-herleidbare URL
    is niet per se verzonnen, maar het model heeft hem niet geopend en niet
    geciteerd — dus onbevestigd. In de praktijk staat er in bijna elke review
    wel één zo'n URL, en dan zegt alleen een boolean te weinig: de lijst maakt
    controleerbaar wélke bron je nog moet natrekken.
    """
    in_text = set(re.findall(r"https?://[^\s)\]>,]+", markdown or ""))
    cited = {c["url"] for c in citations} | set(opened or [])
    if not in_text or not cited:
        return None, []
    def base(u: str) -> str:
        return u.rstrip("/.,);").split("?")[0]
    cited_bases = {base(u) for u in cited}
    unverified = sorted(u for u in in_text if base(u) not in cited_bases)
    return (not unverified), unverified


def run(provider: str, prompt: str, model: str | None = None, retries: int = 3) -> dict[str, Any]:
    """Draai één web-search-call. Gooit niet; fouten komen terug in ``error``."""
    if provider != "openai":
        return {"error": f"provider '{provider}' wordt niet ondersteund"}
    model = model or REVIEW_MODEL
    blank = {"provider": provider, "model": model, "n_searches": 0, "queries": [],
             "latency_s": 0.0, "usage": {}, "cost_usd": {"total": None},
             "source_urls_are_real": None, "unverified_urls": [],
             "prompt_sent": prompt, "raw_markdown": "",
             "citations": [], "sources_consulted": []}

    last_err = None
    for attempt in range(retries):
        t0 = time.time()
        try:
            resp = client().responses.create(
                model=model,
                input=prompt,
                tools=[{"type": "web_search"}],
            )
            payload = resp.model_dump()
            got = _extract(payload)
            raw_usage = payload.get("usage") or {}
            usage = {"input_tokens": raw_usage.get("input_tokens", 0),
                     "output_tokens": raw_usage.get("output_tokens", 0),
                     "total_tokens": raw_usage.get("total_tokens", 0),
                     "cached_tokens": (raw_usage.get("input_tokens_details") or {}).get("cached_tokens", 0),
                     "reasoning_tokens": (raw_usage.get("output_tokens_details") or {}).get("reasoning_tokens", 0)}
            # num_requests is wat OpenAI zelf telt en factureert; het aantal
            # web_search_call-items telt ook geopende pagina's mee en is hoger.
            n_searches = ((payload.get("tool_usage") or {}).get("web_search") or {}).get(
                "num_requests", len(got["queries"]))
            if not got["text"].strip():
                raise RuntimeError("lege response (geen output_text)")
            urls_ok, unverified = _check_urls(got["text"], got["citations"], got["opened"])
            return {**blank,
                    "n_searches": n_searches,
                    "queries": got["queries"],
                    "latency_s": round(time.time() - t0, 2),
                    "usage": usage,
                    "cost_usd": cost_for(model, usage, n_searches),
                    "source_urls_are_real": urls_ok,
                    "unverified_urls": unverified,
                    "raw_markdown": got["text"],
                    "citations": got["citations"],
                    "sources_consulted": list(dict.fromkeys(
                        [c["url"] for c in got["citations"]] + got["opened"])),
                    "error": None}
        except Exception as e:                      # netwerk, rate limit, lege respons
            last_err = f"{type(e).__name__}: {e}"
            if attempt < retries - 1:
                time.sleep(2 ** attempt * 2)
    return {**blank, "error": last_err}
