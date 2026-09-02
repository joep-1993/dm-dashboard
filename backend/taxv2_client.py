"""
Taxonomy API v2 — één client voor het hele dashboard.

WAAROM DIT BESTAND BESTAAT
==========================
De base-URL stond repo-breed ~15 keer hardgecodeerd en er waren drie losse
clients (seo_prio_service, seo_rulings_service, facet_watch_service), elk met
een eigen naam voor dezelfde twee constanten, een eigen `requests.Session`,
een eigen timeout (20 / 30 / 90 s) en — dat is de dure — een eigen idee over
opnieuw proberen. Geen van de drie had retry. Dat is niet cosmetisch: het is de
directe oorzaak van twee bevindingen uit de audit van 2026-09-02.

  * facet_watch schreef bij een enkele 502 `resolution='no_maincat'` als FEIT weg,
    waarna die events onzichtbaar zijn voor de overzichten en het standaardvenster
    (last_ts - 1 dag) ze nooit meer aanraakt.
  * seo_rulings cachet een mislukte `isEnabled`-lookup als `False` voor de rest
    van de run, waardoor die categorie uit elke steekproef verdwijnt.

Eén transiënte fout hoort een retry te zijn, geen permanent antwoord.

CONTRACT
========
* `BASE`           — de base-URL, intern zonder auth.
* `headers()`      — Accept + `X-User-Name`. Die header is verplicht op ELKE
                     schrijfactie (audit-trail) en wordt hier ook op reads gezet,
                     zoals de bestaande clients al deden. Overschrijfbaar met
                     de env-var TAXONOMY_USER_NAME (dat deed facet_watch al).
* `session()`      — gedeelde Session met een urllib3-Retry op 502/503/504 en op
                     connect/read-fouten. `requests.Session` is niet gedocumenteerd
                     als thread-safe, dus we geven er één per thread uit.
* `get()`          — de Response, zodat een aanroeper zelf op status kan sturen.
* `get_json()`     — `raise_for_status()` + `.json()`.

LET OP bij het lezen van facetten van een categorie: gebruik
`/api/Categories/{id}` en lees `facets`, NIET `/api/CategoryFacets`. Dat laatste
laat facetten met `inheritanceStatus=Dependent` stil weg (gemeten 2026-09-02 op
categorie 9003374: 25 tegen 243 facetten).
"""
from __future__ import annotations

import os
import threading
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter

try:                                        # urllib3 v2 en v1 hebben Retry elders
    from urllib3.util.retry import Retry
except ImportError:                         # pragma: no cover
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

BASE = os.getenv(
    "TAXONOMY_API_BASE",
    "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl",
).rstrip("/")

USER_NAME = os.getenv("TAXONOMY_USER_NAME", "SEO_JOEP")

# Default-timeout. Aanroepers met een zwaardere call (de ~146 MB values-dump)
# geven hun eigen mee.
TIMEOUT = 30

_RETRY_STATUS = (502, 503, 504)
_RETRY_TOTAL = 3
_BACKOFF = 0.5          # 0,5s -> 1s -> 2s

_local = threading.local()


def headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {"Accept": "application/json", "X-User-Name": USER_NAME}
    if extra:
        h.update(extra)
    return h


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(headers())
    retry = Retry(
        total=_RETRY_TOTAL,
        connect=_RETRY_TOTAL,
        read=_RETRY_TOTAL,
        status=_RETRY_TOTAL,
        status_forcelist=_RETRY_STATUS,
        # Alleen leesmethodes. Een POST/PUT opnieuw sturen is hier niet veilig:
        # de taxonomie-API kent geen idempotency-key en een PUT die omgevallen is
        # ná het schrijven zou dubbel landen.
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
        backoff_factor=_BACKOFF,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def session() -> requests.Session:
    """Een Session per thread, met retry op transiënte fouten."""
    s = getattr(_local, "session", None)
    if s is None:
        s = _build_session()
        _local.session = s
    return s


def url(path: str) -> str:
    return f"{BASE}/{path.lstrip('/')}"


def get(path: str, params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None, **kw) -> requests.Response:
    """GET met retry. Geeft de Response terug — status zelf afhandelen."""
    return session().get(url(path), params=params,
                         timeout=timeout or TIMEOUT, **kw)


def get_json(path: str, params: Optional[Dict[str, Any]] = None,
             timeout: Optional[float] = None, **kw) -> Any:
    """GET + raise_for_status + json()."""
    r = get(path, params=params, timeout=timeout, **kw)
    r.raise_for_status()
    return r.json()
