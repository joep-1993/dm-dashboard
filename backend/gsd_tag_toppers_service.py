"""
GSD Tag Toppers Service
=======================

Bulk "add-only" onderhoud van tag_toppers-campagnes vanuit een Excel-lijst.

Per regel (shop_id / shop_name / country / productids):

  1. Zoek de `[label:tag_toppers]`-campagne van die shop. Bestaat die niet, maak
     hem aan (PAUSED, zoals GSD_tagtoppers.py) en neem meteen de negatieve
     zoekwoorden over van een zustercampagne van dezelfde shop.
  2. Voeg de product ids toe aan de listing-tree van die campagne. Dit is
     STRIKT ADD-ONLY: bestaande ids blijven staan. Er wordt nooit een boom
     afgebroken en opnieuw opgebouwd — dat is precies waar de losse
     GSD_tagtoppers.py-flow (`rebuild_tree_with_specific_item_ids`) wél toe
     overgaat, en waarom die hier niet gebruikt wordt.
  3. Sluit dezelfde ids uit in ALLE niet-REMOVED zustercampagnes van die shop
     (alles zonder `[label:tag_toppers]`), ook add-only: bestaande uitsluitingen
     blijven bestaan.

Identiteit van een shop = shopnaam ÉN shop_id. `shop_id` alleen is geen sleutel
(652237 is zowel Bruna.nl als Hubfootwear.com), dus daarop matchen zou items van
de ene shop in de campagnes van een andere zetten.

De dry-run leest alleen; er gaat geen enkele mutatie naar Google Ads tenzij
`start_run(dry_run=False)` wordt aangeroepen.
"""
import io
import logging
import re
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from backend.gsd_campaigns_service import (
    GEO_TARGETS,
    TRACKING_TEMPLATES,
    _get_client,
    create_location_op,
    ensure_campaign_label_exists,
    get_negatives,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Direct Shopping accounts (CPR). Zelfde ids als GSD_tagtoppers.py.
COUNTRY_ACCOUNTS = {
    "NL": {"customer_id": "7938980174", "mc_id": "5592708765"},
    "BE": {"customer_id": "2454295509", "mc_id": "5588879919"},
    "DE": {"customer_id": "4192567576", "mc_id": "5342886105"},
}

TAG_TOPPERS_TOKEN = "[label:tag_toppers]"
CHANNEL_TOKEN = "[channel:directshopping]"
TAG_TOPPERS_AD_GROUP = "tag_toppers"
TAG_TOPPERS_SCRIPT_LABEL = "TAGTOPPERS_SCRIPT"

DEFAULT_BID_MICROS = 200_000      # €0,20 — gelijk aan GSD_tagtoppers.py
BUDGET_MICROS = 30_000_000        # €30/dag — gelijk aan GSD_tagtoppers.py

# Google Ads weigert erg grote mutates; ruim onder de limiet blijven.
MUTATE_CHUNK = 1000

# Rijen worden parallel verwerkt: het leeswerk is ~7s per rij en dat is bijna
# helemaal wachten op de API. Elke rij is een andere shop en dus een andere
# campagne, en gelijktijdige rijen op dezelfde shop worden alsnog geserialiseerd
# door _shop_lock.
RUN_WORKERS = 6

# Voorkeur bij meerdere zusters voor het overnemen van negatives.
SIBLING_LABEL_PREF = ["[label:a]", "[label:b]", "[label:c]", "[label:no_ean]", "[label:no_data]"]
NEG_MATCH_ORDER = {"EXACT": 0, "PHRASE": 1, "BROAD": 2}

# Een product id is 27-29 alfanumerieke tekens. De ondergrens van 15 scheidt ze
# van de `number_of_productids`-telcel (4 cijfers), zodat brede rijen — waar die
# telcel achteraan is geschoven — geen aparte behandeling nodig hebben.
_ID_RE = re.compile(r"^[A-Za-z0-9]{15,60}$")
_SPLIT_RE = re.compile(r"[;,|\s]+")

SHOP_RE = re.compile(r"\[shop:([^\]]+)\]")
SHOP_ID_RE = re.compile(r"\[shop_id:([^\]]+)\]")


# ---------------------------------------------------------------------------
# Excel parsing
# ---------------------------------------------------------------------------

def _shop_key(name: Optional[str]) -> str:
    """Normaliseert een shopnaam voor vergelijking tussen campagnes van dezelfde shop.

    Case verschilt tussen campagnes van één shop ([shop:All4fysio.nl] vs
    [shop:all4fysio.nl]) en locale-suffixen zijn dezelfde shop (e5.be|NL ==
    e5.be/nl), dus knippen op de eerste | of /. Bewust conservatief: Decantalo.com
    en Decantalo.de blijven verschillend, net als Ubisoft.com en store.ubisoft.com.
    """
    return re.sub(r"\s*[|/].*$", "", (name or "").strip().lower())


def _clean_shop_name(name: Optional[str]) -> str:
    """Shopnaam zoals hij in een campagnenaam terechtkomt."""
    return (name or "").split("|")[0].strip()


def parse_workbook(data: bytes) -> Dict[str, Any]:
    """Leest de kandidaten-Excel.

    Verwacht kolommen A shop_id, B shop_name, C country, D productids,
    E number_of_productids. Product ids worden uit ALLE cellen vanaf kolom D
    gehaald en gesplitst op komma/puntkomma/pipe/whitespace, zodat een rij die
    over honderden cellen is uitgesmeerd (Excel's celgrens van 32.767 tekens)
    vanzelf goed gaat.
    """
    from openpyxl import load_workbook

    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for excel_row, raw in enumerate(ws.iter_rows(values_only=True), start=1):
        if excel_row == 1:
            continue  # header
        if not raw or not any(v not in (None, "") for v in raw):
            continue

        shop_id = raw[0] if len(raw) > 0 else None
        shop_name = raw[1] if len(raw) > 1 else None
        country = raw[2] if len(raw) > 2 else None

        ids: "OrderedDict[str, None]" = OrderedDict()
        for cell in raw[3:]:
            if cell in (None, ""):
                continue
            for token in _SPLIT_RE.split(str(cell)):
                token = token.strip().strip('"').strip("'")
                if token and _ID_RE.match(token):
                    ids[token] = None

        shop_id_s = str(shop_id).strip() if shop_id is not None else ""
        shop_name_s = str(shop_name).strip() if shop_name else ""
        country_s = str(country).strip().upper() if country else ""

        if not shop_id_s or not shop_name_s:
            warnings.append(f"Rij {excel_row}: shop_id of shop_name ontbreekt — overgeslagen")
            continue
        if country_s not in COUNTRY_ACCOUNTS:
            warnings.append(f"Rij {excel_row}: onbekend land {country_s!r} — overgeslagen")
            continue
        if not ids:
            warnings.append(f"Rij {excel_row}: geen product ids gevonden — overgeslagen")
            continue

        stated = raw[4] if len(raw) > 4 else None
        stated_n = stated if isinstance(stated, int) else None
        if stated_n is not None and stated_n != len(ids):
            warnings.append(
                f"Rij {excel_row} ({shop_name_s} {country_s}): telcel zegt {stated_n} "
                f"maar er zijn {len(ids)} unieke ids gevonden"
            )

        rows.append({
            "excel_row": excel_row,
            "shop_id": shop_id_s,
            "shop_name": shop_name_s,
            "country": country_s,
            "item_ids": list(ids),
        })

    wb.close()
    return {
        "rows": rows,
        "warnings": warnings,
        "total_rows": len(rows),
        "total_ids": sum(len(r["item_ids"]) for r in rows),
    }


# ---------------------------------------------------------------------------
# Google Ads reads
# ---------------------------------------------------------------------------

def _customer_id(country: str) -> str:
    acct = COUNTRY_ACCOUNTS.get((country or "").upper())
    if not acct:
        raise ValueError(f"Onbekend land {country!r}")
    return acct["customer_id"]


def _fetch_shop_campaigns(client, customer_id: str, shop_id: str, shop_name: str) -> Dict[str, List[dict]]:
    """Alle niet-REMOVED campagnes van deze shop, gesplitst in tag_toppers en zusters.

    Matcht op shop_id (GAQL) en daarna op genormaliseerde shopnaam in Python,
    omdat GAQL's LIKE case-sensitive is en de naam per campagne in case verschilt.
    """
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT campaign.id, campaign.name, campaign.resource_name, campaign.status
        FROM campaign
        WHERE campaign.name LIKE '%shop_id:{shop_id}]%'
          AND campaign.status != 'REMOVED'
    """
    want = _shop_key(shop_name)
    tag_toppers: List[dict] = []
    siblings: List[dict] = []
    for row in ga.search(customer_id=customer_id, query=q):
        name = row.campaign.name
        m = SHOP_RE.search(name)
        if not m or _shop_key(m.group(1)) != want:
            continue
        entry = {
            "id": row.campaign.id,
            "name": name,
            "resource": row.campaign.resource_name,
            "status": row.campaign.status.name,
        }
        if TAG_TOPPERS_TOKEN in name.lower():
            tag_toppers.append(entry)
        else:
            siblings.append(entry)
    tag_toppers.sort(key=lambda c: (c["status"] != "ENABLED", c["id"]))
    siblings.sort(key=lambda c: c["id"])
    return {"tag_toppers": tag_toppers, "siblings": siblings}


def _read_campaign_tree(client, customer_id: str, campaign_id: int) -> Dict[str, Dict[str, dict]]:
    """{ad_group_id: {resource_name: node}} voor alle listing-groups in een campagne.

    Eén query per campagne in plaats van één per ad group: een GSD-campagne heeft
    14 prijsbucket-ad-groups, dus dat scheelt bij 622 rijen duizenden calls.
    """
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT
          ad_group.id,
          ad_group.name,
          ad_group.status,
          ad_group_criterion.resource_name,
          ad_group_criterion.listing_group.type,
          ad_group_criterion.listing_group.parent_ad_group_criterion,
          ad_group_criterion.listing_group.case_value.product_custom_attribute.index,
          ad_group_criterion.listing_group.case_value.product_custom_attribute.value,
          ad_group_criterion.listing_group.case_value.product_item_id.value,
          ad_group_criterion.negative,
          ad_group_criterion.cpc_bid_micros
        FROM ad_group_criterion
        WHERE campaign.id = {campaign_id}
          AND ad_group_criterion.type = 'LISTING_GROUP'
          AND ad_group_criterion.status != 'REMOVED'
          AND ad_group.status != 'REMOVED'
    """
    out: Dict[str, Dict[str, dict]] = {}
    for row in ga.search(customer_id=customer_id, query=q):
        agc = row.ad_group_criterion
        lg = agc.listing_group
        if lg.type_.name not in ("SUBDIVISION", "UNIT"):
            continue
        cv = lg.case_value
        which = cv._pb.WhichOneof("dimension")
        dim = index = value = item_id = None
        if which == "product_item_id":
            dim = "item_id"
            item_id = cv.product_item_id.value  # "" => OTHERS
        elif which == "product_custom_attribute":
            dim = "custom_attr"
            index = cv.product_custom_attribute.index.name
            value = cv.product_custom_attribute.value
        ag_id = str(row.ad_group.id)
        out.setdefault(ag_id, {})[agc.resource_name] = {
            "resource": agc.resource_name,
            "ad_group_id": ag_id,
            "ad_group_name": row.ad_group.name,
            "type": lg.type_.name,
            "parent": lg.parent_ad_group_criterion or None,
            "dim": dim,
            "index": index,
            "value": value,
            "item_id": item_id,
            "negative": bool(agc.negative),
            "bid": int(agc.cpc_bid_micros or 0),
        }
    return out


def _children(nodes: Dict[str, dict], parent_resource: Optional[str]) -> List[dict]:
    return [n for n in nodes.values() if n["parent"] == parent_resource]


def _root(nodes: Dict[str, dict]) -> Optional[dict]:
    for n in nodes.values():
        if not n["parent"]:
            return n
    return None


# ---------------------------------------------------------------------------
# Planning: wat zou er gebeuren
# ---------------------------------------------------------------------------

def _plan_tag_toppers_adds(nodes: Dict[str, dict], item_ids: List[str]) -> Dict[str, Any]:
    """Welke ids ontbreken nog als POSITIEVE unit onder de root van de tag_toppers-boom."""
    root = _root(nodes)
    if root is None:
        return {"root": None, "existing": set(), "missing": list(item_ids)}
    # Positief én negatief: Google staat maar één node per case value toe, dus een
    # id dat al als negatief onder deze root hangt kan er niet positief bij.
    existing = {
        n["item_id"] for n in nodes.values()
        if n["dim"] == "item_id" and n["item_id"]
    }
    missing = [i for i in item_ids if i not in existing]
    return {"root": root, "existing": existing, "missing": missing}


def _item_id_containers(nodes: Dict[str, dict]) -> List[dict]:
    """SUBDIVISIONs die al een item-id niveau hebben (dus waar een negatief id bij kan)."""
    containers = []
    for n in nodes.values():
        if n["type"] != "SUBDIVISION":
            continue
        kids = _children(nodes, n["resource"])
        if any(k["dim"] == "item_id" for k in kids):
            containers.append(n)
    return containers


def _convertible_leaves(nodes: Dict[str, dict]) -> List[dict]:
    """Positieve biddable UNIT-leaves die nog géén item-id niveau onder zich hebben.

    Die moeten een SUBDIVISION worden met item-id OTHERS (positief, originele bid)
    plus de negatieve ids. NEGATIEVE units zijn uitsluitingen en blijven met rust —
    die naar subdivisions omzetten zou bestaande uitsluitingen wissen.
    """
    leaves = []
    for n in nodes.values():
        if n["type"] != "UNIT" or n["negative"]:
            continue
        if n["dim"] == "item_id":
            continue  # zit al op item-id niveau
        leaves.append(n)
    return leaves


def _plan_sibling_exclusions(nodes: Dict[str, dict], item_ids: List[str]) -> Dict[str, Any]:
    """Plan per ad group: waar komen de negatieve item-ids terecht.

    Twee vormen, gelijk aan wat de listing-tree van GSD kent:
      * er is al een item-id niveau  -> negatieve unit erbij hangen (goedkoop)
      * de leaf is een biddable UNIT -> omzetten naar SUBDIVISION met item-id
                                        OTHERS (positief, originele bid) + negatieven
    """
    containers = _item_id_containers(nodes)
    appends: List[Dict[str, Any]] = []
    converts: List[Dict[str, Any]] = []

    if containers:
        for c in containers:
            kids = _children(nodes, c["resource"])
            # Ook de POSITIEVE item-ids meetellen: één node per case value, dus een
            # id dat er positief hangt kan er niet negatief bij en levert anders
            # LISTING_GROUP_ALREADY_EXISTS op.
            already = {k["item_id"] for k in kids if k["dim"] == "item_id" and k["item_id"]}
            missing = [i for i in item_ids if i not in already]
            if missing:
                appends.append({"parent": c["resource"], "missing": missing})
    else:
        for leaf in _convertible_leaves(nodes):
            converts.append({"leaf": leaf, "missing": list(item_ids)})

    return {
        "appends": appends,
        "converts": converts,
        "n_new": sum(len(a["missing"]) for a in appends) + sum(len(c["missing"]) for c in converts),
    }


# ---------------------------------------------------------------------------
# Negatives overnemen van een zustercampagne
# ---------------------------------------------------------------------------

def _neg_key(text: str, match_type: str) -> Tuple[str, str]:
    """Google matcht negatives case-insensitief maar bewaart 'emma' en 'Emma' wél
    als aparte criteria; dedupliceer daarom op (lowercase tekst, match type)."""
    return (str(text).strip().lower(), match_type)


def _fetch_campaign_negatives(client, customer_id: str, campaign_id: int) -> "OrderedDict":
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT campaign_criterion.keyword.text,
               campaign_criterion.keyword.match_type,
               campaign_criterion.status
        FROM campaign_criterion
        WHERE campaign.id = {campaign_id}
          AND campaign_criterion.type = 'KEYWORD'
          AND campaign_criterion.negative = TRUE
    """
    out: "OrderedDict" = OrderedDict()
    for row in ga.search(customer_id=customer_id, query=q):
        cc = row.campaign_criterion
        if cc.status.name == "REMOVED":
            continue
        out.setdefault(_neg_key(cc.keyword.text, cc.keyword.match_type.name),
                       (cc.keyword.text, cc.keyword.match_type.name))
    return out


def _pick_negatives_source(siblings: List[dict]) -> Optional[dict]:
    """ENABLED zuster heeft voorkeur, PAUSED is de fallback; daarbinnen label:a, b, c, ..."""
    for status in ("ENABLED", "PAUSED"):
        pool = [s for s in siblings if s["status"] == status and CHANNEL_TOKEN in s["name"].lower()]
        if not pool:
            continue
        for lbl in SIBLING_LABEL_PREF:
            hits = sorted([s for s in pool if lbl in s["name"].lower()], key=lambda s: s["id"])
            if hits:
                return hits[0]
        return sorted(pool, key=lambda s: s["id"])[0]
    return None


def _copy_negatives(client, customer_id: str, target_resource: str, target_id: int,
                    source: dict) -> Tuple[int, List[str]]:
    """Neemt de negatives van `source` over in de doelcampagne. Idempotent."""
    src = _fetch_campaign_negatives(client, customer_id, source["id"])
    dst = _fetch_campaign_negatives(client, customer_id, target_id)
    missing = [v for k, v in src.items() if k not in dst]
    missing.sort(key=lambda x: (NEG_MATCH_ORDER.get(x[1], 9), x[0].lower()))
    if not missing:
        return 0, []

    svc = client.get_service("CampaignCriterionService")
    ops = []
    for text, mt in missing:
        op = client.get_type("CampaignCriterionOperation")
        c = op.create
        c.campaign = target_resource
        c.negative = True
        c.keyword.text = text
        c.keyword.match_type = client.enums.KeywordMatchTypeEnum[mt]
        ops.append(op)

    added, errors = 0, []
    for i in range(0, len(ops), 200):
        chunk = ops[i:i + 200]
        # partial_failure MOET via een request-object; als kwarg wordt het geweigerd
        # door google-ads v28/v29.
        req = client.get_type("MutateCampaignCriteriaRequest")
        req.customer_id = str(customer_id)
        req.operations.extend(chunk)
        req.partial_failure = True
        try:
            resp = svc.mutate_campaign_criteria(request=req)
        except GoogleAdsException as ex:
            errors.append(_err(ex))
            continue
        added += sum(1 for r in resp.results if r.resource_name)
    return added, errors


# ---------------------------------------------------------------------------
# Google Ads writes
# ---------------------------------------------------------------------------

class _Temp:
    """Per-mutate generator van tijdelijke (negatieve) criterion-ids."""

    def __init__(self):
        self.n = 0

    def path(self, client, customer_id, ad_group_id) -> str:
        self.n -= 1
        return client.get_service("AdGroupCriterionService").ad_group_criterion_path(
            customer_id, str(ad_group_id), str(self.n))


def _unit_op(client, customer_id, ad_group_id, temp, parent_resource, *,
             item_id_value=None, custom_attr=None, negative=False, bid=None):
    op = client.get_type("AdGroupCriterionOperation")
    cr = op.create
    cr.resource_name = temp.path(client, customer_id, ad_group_id)
    cr.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    if bid and not negative:
        cr.cpc_bid_micros = bid
    lg = cr.listing_group
    lg.type_ = client.enums.ListingGroupTypeEnum.UNIT
    if parent_resource:
        lg.parent_ad_group_criterion = parent_resource
    if item_id_value is not None:
        if item_id_value != "":
            lg.case_value.product_item_id.value = item_id_value
        else:
            client.copy_from(lg.case_value.product_item_id, client.get_type("ProductItemIdInfo"))
    elif custom_attr is not None:
        lg.case_value.product_custom_attribute.index = \
            client.enums.ProductCustomAttributeIndexEnum[custom_attr["index"]]
        if custom_attr["value"]:
            lg.case_value.product_custom_attribute.value = custom_attr["value"]
    if negative:
        cr.negative = True
    return op, cr.resource_name


def _subdiv_op(client, customer_id, ad_group_id, temp, parent_resource, *, custom_attr):
    op = client.get_type("AdGroupCriterionOperation")
    cr = op.create
    cr.resource_name = temp.path(client, customer_id, ad_group_id)
    cr.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    lg = cr.listing_group
    lg.type_ = client.enums.ListingGroupTypeEnum.SUBDIVISION
    if parent_resource:
        lg.parent_ad_group_criterion = parent_resource
    if custom_attr is not None:
        lg.case_value.product_custom_attribute.index = \
            client.enums.ProductCustomAttributeIndexEnum[custom_attr["index"]]
        if custom_attr["value"]:
            lg.case_value.product_custom_attribute.value = custom_attr["value"]
    return op, cr.resource_name


def _remove_op(client, resource_name):
    op = client.get_type("AdGroupCriterionOperation")
    op.remove = resource_name
    return op


def _dedupe_msgs(msgs: List[str]) -> List[str]:
    """Identieke meldingen samenvouwen tot 'melding (3x)'.

    Eén mislukte mutate levert per operatie dezelfde regel op; ongefilterd werd dat
    een muur van drie keer dezelfde zin, drie keer herhaald.
    """
    counts: "OrderedDict[str, int]" = OrderedDict()
    for m in msgs:
        key = str(m).strip()
        counts[key] = counts.get(key, 0) + 1
    return [f"{k} ({v}x)" if v > 1 else k for k, v in counts.items()]


def _err(ex: Exception) -> str:
    if isinstance(ex, GoogleAdsException):
        try:
            return "; ".join(e.message for e in ex.failure.errors)[:400]
        except Exception:
            return str(ex)[:400]
    return f"{type(ex).__name__}: {ex}"[:400]


def _is_concurrent_modification(ex: GoogleAdsException) -> bool:
    """CONCURRENT_MODIFICATION is transient: Google was nog bezig met een eerdere
    mutate op dezelfde ad group. Opnieuw proberen lost het op."""
    try:
        for e in ex.failure.errors:
            code = e.error_code
            if getattr(code, "database_error", None) and \
                    code.database_error.name == "CONCURRENT_MODIFICATION":
                return True
            if "same resource at once" in (e.message or ""):
                return True
    except Exception:
        pass
    return False


def _is_already_exists(ex: GoogleAdsException) -> bool:
    """LISTING_GROUP_ALREADY_EXISTS is geen fout maar een no-op: de node staat er al.

    Dat is precies wat add-only hoort te doen, dus zo'n operatie telt als
    overgeslagen. Kan alsnog voorkomen als de boom een id al POSITIEF bevat waar wij
    een negatief willen zetten — Google staat maar één node per case value toe,
    ongeacht de negative-vlag.
    """
    try:
        for e in ex.failure.errors:
            code = e.error_code
            if getattr(code, "criterion_error", None) and \
                    code.criterion_error.name == "LISTING_GROUP_ALREADY_EXISTS":
                return True
            if "already exists" in (e.message or "").lower():
                return True
    except Exception:
        pass
    return False


def _mutate_with_retry(client, customer_id: str, ops: List[Any], retries: int = 4):
    """Voert één mutate uit en herhaalt bij CONCURRENT_MODIFICATION met backoff.

    partial_failure staat aan zodat één afgekeurde operatie niet het hele blok van
    maximaal MUTATE_CHUNK sloopt — zonder dat kostte één 'bestaat al' tot 1000
    geldige mutaties.
    """
    svc = client.get_service("AdGroupCriterionService")
    delay = 2.0
    last = None
    for attempt in range(retries):
        req = client.get_type("MutateAdGroupCriteriaRequest")
        req.customer_id = str(customer_id)
        req.operations.extend(ops)
        req.partial_failure = True
        try:
            return svc.mutate_ad_group_criteria(request=req), None
        except GoogleAdsException as ex:
            last = ex
            if not _is_concurrent_modification(ex) or attempt == retries - 1:
                break
            logger.warning("CONCURRENT_MODIFICATION, opnieuw over %.0fs (poging %d/%d)",
                           delay, attempt + 1, retries)
            time.sleep(delay)
            delay *= 2
    return None, last


def _read_partial_failure(client, resp) -> Tuple[int, List[str], set]:
    """(overgeslagen, echte fouten, mislukte indexen) uit een partial_failure-respons."""
    skipped, errors, failed = 0, [], set()
    pfe = getattr(resp, "partial_failure_error", None)
    if not pfe or not pfe.details:
        return skipped, errors, failed
    failure_type = client.get_type("GoogleAdsFailure")
    for det in pfe.details:
        try:
            f = type(failure_type).deserialize(det.value)
        except Exception:
            continue
        for err in f.errors:
            idx = None
            for el in err.location.field_path_elements:
                if el.field_name == "operations":
                    idx = el.index
            if idx is not None:
                failed.add(idx)
            code = err.error_code
            is_dup = (getattr(code, "criterion_error", None)
                      and code.criterion_error.name == "LISTING_GROUP_ALREADY_EXISTS") \
                or "already exists" in (err.message or "").lower()
            if is_dup:
                skipped += 1
            else:
                errors.append(err.message)
    return skipped, errors, failed


def _mutate_criteria(client, customer_id: str, ops: List[Any]) -> Tuple[int, int, List[str]]:
    """Voert criterion-operaties uit in blokken.

    Geeft (geland, overgeslagen, fouten) terug. 'Overgeslagen' is uitsluitend
    LISTING_GROUP_ALREADY_EXISTS — de node stond er al, wat bij een add-only tool
    het gewenste eindresultaat is en dus geen fout.
    """
    done, skipped, errors = 0, 0, []
    for i in range(0, len(ops), MUTATE_CHUNK):
        chunk = ops[i:i + MUTATE_CHUNK]
        resp, ex = _mutate_with_retry(client, customer_id, chunk)
        if resp is None:
            if _is_already_exists(ex):
                skipped += len(chunk)
            else:
                errors.append(_err(ex))
            continue
        s, errs, failed = _read_partial_failure(client, resp)
        skipped += s
        errors.extend(errs)
        done += sum(1 for j, r in enumerate(resp.results)
                    if r.resource_name and j not in failed)
    return done, skipped, errors


def _apply_tag_toppers_adds(client, customer_id: str, ad_group_id: str,
                            root_resource: str, missing: List[str]) -> Tuple[int, int, List[str]]:
    """Hangt ontbrekende ids als POSITIEVE units onder de bestaande root. Add-only."""
    if not missing:
        return 0, 0, []
    temp = _Temp()
    ops = []
    for item_id in missing:
        op, _ = _unit_op(client, customer_id, ad_group_id, temp, root_resource,
                         item_id_value=item_id, negative=False, bid=DEFAULT_BID_MICROS)
        ops.append(op)
    return _mutate_criteria(client, customer_id, ops)


def _apply_sibling_exclusions(client, customer_id: str, ad_group_id: str,
                              plan: Dict[str, Any]) -> Tuple[int, int, List[str]]:
    """Voegt negatieve item-ids toe. Bestaande uitsluitingen blijven staan."""
    done, skipped, errors = 0, 0, []

    for app in plan["appends"]:
        temp = _Temp()
        ops = []
        for item_id in app["missing"]:
            op, _ = _unit_op(client, customer_id, ad_group_id, temp, app["parent"],
                             item_id_value=item_id, negative=True)
            ops.append(op)
        d, s, e = _mutate_criteria(client, customer_id, ops)
        done += d
        skipped += s
        errors.extend(e)

    for conv in plan["converts"]:
        leaf = conv["leaf"]
        # Stap 1 is atomisch: de biddable leaf verdwijnt en wordt in dezelfde
        # mutate vervangen door een subdivision met item-id OTHERS, zodat de ad
        # group nooit even zonder dat targeting-pad zit.
        temp = _Temp()
        ca = {"index": leaf["index"], "value": leaf["value"] or ""} if leaf["dim"] == "custom_attr" else None
        bid = leaf["bid"] or DEFAULT_BID_MICROS
        ops = [_remove_op(client, leaf["resource"])]
        sub_op, sub_res = _subdiv_op(client, customer_id, ad_group_id, temp,
                                     leaf["parent"], custom_attr=ca)
        ops.append(sub_op)
        others_op, _ = _unit_op(client, customer_id, ad_group_id, temp, sub_res,
                                item_id_value="", negative=False, bid=bid)
        ops.append(others_op)
        resp, ex = _mutate_with_retry(client, customer_id, ops)
        if resp is None:
            if _is_already_exists(ex):
                skipped += 1
            else:
                errors.append(f"convert {leaf['resource']}: {_err(ex)}")
            continue
        real_sub = resp.results[1].resource_name

        temp2 = _Temp()
        neg_ops = []
        for item_id in conv["missing"]:
            op, _ = _unit_op(client, customer_id, ad_group_id, temp2, real_sub,
                             item_id_value=item_id, negative=True)
            neg_ops.append(op)
        d, s, e = _mutate_criteria(client, customer_id, neg_ops)
        done += d
        skipped += s
        errors.extend(e)

    return done, skipped, errors


def _create_tag_toppers_campaign(client, customer_id: str, country: str,
                                 shop_id: str, shop_name: str) -> Tuple[Optional[dict], List[str]]:
    """Maakt een tag_toppers-campagne aan (PAUSED) met ad group, boomwortel en ad.

    Bewust NIET via gsd_campaigns_service.add_standard_shopping_campaign: die zet
    `feed_label` en een ander budget, terwijl de bestaande tag_toppers-campagnes
    dat niet hebben. Hier wordt de conventie van GSD_tagtoppers.py aangehouden.
    """
    errors: List[str] = []
    base_shop = _clean_shop_name(shop_name)
    campaign_name = (f"[shop:{base_shop}] [shop_id:{shop_id}] "
                     f"[channel:directshopping] [label:tag_toppers]")
    mc_id = COUNTRY_ACCOUNTS[country]["mc_id"]

    budget_service = client.get_service("CampaignBudgetService")
    budget_op = client.get_type("CampaignBudgetOperation")
    budget = budget_op.create
    budget.name = f"budget_{base_shop}_{shop_id}_directshopping_tag_toppers_{int(time.time())}"
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.amount_micros = BUDGET_MICROS
    budget.explicitly_shared = False
    try:
        budget_res = budget_service.mutate_campaign_budgets(
            customer_id=customer_id, operations=[budget_op]).results[0].resource_name
    except GoogleAdsException as ex:
        return None, [f"budget: {_err(ex)}"]

    campaign_service = client.get_service("CampaignService")
    camp_op = client.get_type("CampaignOperation")
    camp = camp_op.create
    camp.name = campaign_name
    camp.campaign_budget = budget_res
    camp.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SHOPPING
    camp.shopping_setting.merchant_id = int(mc_id)
    camp.shopping_setting.campaign_priority = 0
    camp.shopping_setting.enable_local = True
    camp.tracking_url_template = TRACKING_TEMPLATES[country]
    camp.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )
    camp.status = client.enums.CampaignStatusEnum.PAUSED
    camp.manual_cpc.enhanced_cpc_enabled = False
    try:
        camp_res = campaign_service.mutate_campaigns(
            customer_id=customer_id, operations=[camp_op]).results[0].resource_name
    except GoogleAdsException as ex:
        return None, [f"campagne: {_err(ex)}"]

    campaign_id = int(camp_res.split("/")[-1])
    time.sleep(2)  # laten propageren voor de child-objecten

    try:
        crit_service = client.get_service("CampaignCriterionService")
        crit_service.mutate_campaign_criteria(
            customer_id=customer_id, operations=[create_location_op(client, camp_res, country)])
    except Exception as ex:
        errors.append(f"geo: {_err(ex)}")

    try:
        label_res = ensure_campaign_label_exists(client, customer_id, TAG_TOPPERS_SCRIPT_LABEL)
        if label_res:
            lbl_op = client.get_type("CampaignLabelOperation")
            lbl_op.create.campaign = camp_res
            lbl_op.create.label = label_res
            client.get_service("CampaignLabelService").mutate_campaign_labels(
                customer_id=customer_id, operations=[lbl_op])
    except Exception as ex:
        errors.append(f"label: {_err(ex)}")

    ad_group_service = client.get_service("AdGroupService")
    ag_op = client.get_type("AdGroupOperation")
    ag = ag_op.create
    ag.campaign = camp_res
    ag.name = TAG_TOPPERS_AD_GROUP
    ag.cpc_bid_micros = DEFAULT_BID_MICROS
    ag.status = client.enums.AdGroupStatusEnum.ENABLED
    try:
        ag_res = ad_group_service.mutate_ad_groups(
            customer_id=customer_id, operations=[ag_op]).results[0].resource_name
    except GoogleAdsException as ex:
        return None, errors + [f"ad group: {_err(ex)}"]

    ad_group_id = ag_res.split("/")[-1]
    time.sleep(1)

    # Boomwortel: root SUBDIVISION + item-id OTHERS NEGATIEF. Daarmee toont de
    # campagne niets tot de ids eronder gehangen worden (inclusieve logica).
    temp = _Temp()
    root_op, root_tmp = _subdiv_op(client, customer_id, ad_group_id, temp, None, custom_attr=None)
    others_op, _ = _unit_op(client, customer_id, ad_group_id, temp, root_tmp,
                            item_id_value="", negative=True)
    try:
        agc = client.get_service("AdGroupCriterionService")
        resp = agc.mutate_ad_group_criteria(customer_id=customer_id, operations=[root_op, others_op])
        root_resource = resp.results[0].resource_name
    except GoogleAdsException as ex:
        return None, errors + [f"boomwortel: {_err(ex)}"]

    time.sleep(1)
    try:
        ad_op = client.get_type("AdGroupAdOperation")
        ad = ad_op.create
        ad.ad_group = ag_res
        ad.status = client.enums.AdGroupAdStatusEnum.ENABLED
        client.copy_from(ad.ad.shopping_product_ad, client.get_type("ShoppingProductAdInfo"))
        client.get_service("AdGroupAdService").mutate_ad_group_ads(
            customer_id=customer_id, operations=[ad_op])
    except GoogleAdsException as ex:
        errors.append(f"shopping ad: {_err(ex)}")

    return {
        "id": campaign_id,
        "name": campaign_name,
        "resource": camp_res,
        "status": "PAUSED",
        "ad_group_id": ad_group_id,
        "root_resource": root_resource,
    }, errors


# ---------------------------------------------------------------------------
# Run orchestration
# ---------------------------------------------------------------------------

_state_lock = threading.Lock()
_state: Dict[str, Any] = {
    "running": False,
    "dry_run": True,
    "current": 0,
    "total": 0,
    "cancel": False,
    "results": [],
    "started_at": None,
    "finished_at": None,
    "summary": {},
}


def get_progress() -> Dict[str, Any]:
    with _state_lock:
        return {
            "running": _state["running"],
            "dry_run": _state["dry_run"],
            "current": _state["current"],
            "total": _state["total"],
            "started_at": _state["started_at"],
            "finished_at": _state["finished_at"],
        }


def get_results() -> Dict[str, Any]:
    with _state_lock:
        return {
            "running": _state["running"],
            "dry_run": _state["dry_run"],
            "results": list(_state["results"]),
            "summary": dict(_state["summary"]),
        }


def cancel_run() -> None:
    with _state_lock:
        _state["cancel"] = True


def _process_row(client, row: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
    """Eén Excel-regel: plannen en (als dry_run False) uitvoeren."""
    country = row["country"]
    customer_id = _customer_id(country)
    item_ids = row["item_ids"]

    res: Dict[str, Any] = {
        "excel_row": row["excel_row"],
        "shop_id": row["shop_id"],
        "shop_name": row["shop_name"],
        "country": country,
        "n_ids": len(item_ids),
        "campaign_action": "",
        "campaign_name": "",
        "ids_already_present": 0,
        "ids_to_add": 0,
        "ids_added": 0,
        "siblings": 0,
        "sibling_ad_groups": 0,
        "exclusions_to_add": 0,
        "exclusions_added": 0,
        "negatives_source": "",
        "negatives_copied": 0,
        "status": "ok",
        "errors": [],
        # Per campagne/ad group wat er gepland en wat er echt geland is. De tabel
        # klapt hierop uit, zodat een run niet alleen een totaal maar ook een
        # aanwijsbare plek van mislukking geeft.
        "targets": [],
    }

    def target(kind, campaign, planned, applied=0, ad_group_id=None, errors=None,
               note="", skipped=0):
        errs = _dedupe_msgs([str(e)[:300] for e in (errors or [])])
        if errs:
            status = "fout" if applied == 0 else "deels"
        elif dry_run:
            status = "gepland"
        elif skipped and applied == 0:
            # Niets geschreven omdat alles er al stond: dat is het gewenste
            # eindresultaat van een add-only tool, geen mislukking.
            status = "overgeslagen"
        elif applied + skipped >= planned:
            status = "ok"
        else:
            status = "deels" if applied else "fout"
        res["targets"].append({
            "kind": kind,
            "campaign": campaign,
            "ad_group_id": ad_group_id,
            "planned": planned,
            "applied": applied,
            "skipped": skipped,
            "status": status,
            "note": note,
            "errors": errs[:5],
        })

    camps = _fetch_shop_campaigns(client, customer_id, row["shop_id"], row["shop_name"])
    siblings = camps["siblings"]
    res["siblings"] = len(siblings)

    # ---- 1/2: tag_toppers-campagne -------------------------------------
    tt = camps["tag_toppers"][0] if camps["tag_toppers"] else None

    if tt is None:
        res["campaign_action"] = "aanmaken"
        res["campaign_name"] = (f"[shop:{_clean_shop_name(row['shop_name'])}] "
                                f"[shop_id:{row['shop_id']}] [channel:directshopping] "
                                f"[label:tag_toppers]")
        res["ids_to_add"] = len(item_ids)
        source = _pick_negatives_source(siblings)
        res["negatives_source"] = source["name"] if source else "geen zuster gevonden"

        if not dry_run:
            created, errs = _create_tag_toppers_campaign(
                client, customer_id, country, row["shop_id"], row["shop_name"])
            res["errors"].extend(errs)
            if created is None:
                target("campagne aanmaken", res["campaign_name"], 1, 0, errors=errs)
                res["status"] = "fout"
                return res
            res["campaign_name"] = created["name"]
            target("campagne aanmaken", created["name"], 1, 1, errors=errs, note="PAUSED")
            added, skipped, errs2 = _apply_tag_toppers_adds(
                client, customer_id, created["ad_group_id"], created["root_resource"], item_ids)
            res["ids_added"] = added
            res["errors"].extend(errs2)
            target("ids toevoegen", created["name"], len(item_ids), added,
                   ad_group_id=created["ad_group_id"], errors=errs2, skipped=skipped)
            if source:
                n, errs3 = _copy_negatives(client, customer_id, created["resource"],
                                           created["id"], source)
                res["negatives_copied"] = n
                res["errors"].extend(errs3)
                target("negatives overnemen", created["name"], n, n, errors=errs3,
                       note=f"bron: {source['name']}")
        else:
            target("campagne aanmaken", res["campaign_name"], 1, 0, note="PAUSED")
            target("ids toevoegen", res["campaign_name"], len(item_ids), 0)
            if source:
                src_negs = _fetch_campaign_negatives(client, customer_id, source["id"])
                res["negatives_copied"] = len(src_negs)
                target("negatives overnemen", res["campaign_name"], len(src_negs), 0,
                       note=f"bron: {source['name']}")
    else:
        res["campaign_action"] = "bestaat"
        res["campaign_name"] = tt["name"]
        trees = _read_campaign_tree(client, customer_id, tt["id"])
        # de tag_toppers-campagne heeft één ad group; pak de grootste boom als er meer zijn
        ag_id, nodes = (max(trees.items(), key=lambda kv: len(kv[1]))
                        if trees else (None, {}))
        plan = _plan_tag_toppers_adds(nodes, item_ids)
        res["ids_already_present"] = len(item_ids) - len(plan["missing"])
        res["ids_to_add"] = len(plan["missing"])

        if not dry_run and plan["missing"]:
            if plan["root"] is None:
                msg = "geen listing-tree gevonden in de tag_toppers ad group"
                res["errors"].append(msg)
                res["status"] = "fout"
                target("ids toevoegen", tt["name"], len(plan["missing"]), 0,
                       ad_group_id=ag_id, errors=[msg])
            else:
                added, skipped, errs = _apply_tag_toppers_adds(
                    client, customer_id, ag_id, plan["root"]["resource"], plan["missing"])
                res["ids_added"] = added
                res["errors"].extend(errs)
                target("ids toevoegen", tt["name"], len(plan["missing"]), added,
                       ad_group_id=ag_id, errors=errs, skipped=skipped)
        elif plan["missing"]:
            target("ids toevoegen", tt["name"], len(plan["missing"]), 0, ad_group_id=ag_id)
        else:
            target("ids toevoegen", tt["name"], 0, 0, ad_group_id=ag_id,
                   note="alle ids stonden er al")

    # ---- 3: uitsluiten in de zustercampagnes ---------------------------
    for sib in siblings:
        try:
            trees = _read_campaign_tree(client, customer_id, sib["id"])
        except GoogleAdsException as ex:
            msg = _err(ex)
            res["errors"].append(f"{sib['name']}: {msg}")
            target("uitsluiten", sib["name"], 0, 0, errors=[msg])
            continue
        for ag_id, nodes in trees.items():
            if not nodes:
                continue
            res["sibling_ad_groups"] += 1
            plan = _plan_sibling_exclusions(nodes, item_ids)
            res["exclusions_to_add"] += plan["n_new"]
            if not dry_run and plan["n_new"]:
                done, skipped, errs = _apply_sibling_exclusions(client, customer_id, ag_id, plan)
                res["exclusions_added"] += done
                res["errors"].extend(f"{sib['name']}/{ag_id}: {e}" for e in errs)
                target("uitsluiten", sib["name"], plan["n_new"], done,
                       ad_group_id=ag_id, errors=errs, skipped=skipped)
            else:
                target("uitsluiten", sib["name"], plan["n_new"], 0, ad_group_id=ag_id,
                       note="niets te doen" if not plan["n_new"] else "")

    if res["errors"]:
        res["status"] = "fout" if res["status"] == "fout" else "deels"
    res["errors"] = _dedupe_msgs(res["errors"])[:10]
    return res


def _failed_row(row: Dict[str, Any], message: str) -> Dict[str, Any]:
    return {
        "excel_row": row.get("excel_row"),
        "shop_id": row.get("shop_id"),
        "shop_name": row.get("shop_name"),
        "country": row.get("country"),
        "n_ids": len(row.get("item_ids") or []),
        "campaign_action": "", "campaign_name": "",
        "ids_already_present": 0, "ids_to_add": 0, "ids_added": 0,
        "siblings": 0, "sibling_ad_groups": 0,
        "exclusions_to_add": 0, "exclusions_added": 0,
        "negatives_source": "", "negatives_copied": 0,
        "status": "fout", "errors": [message], "targets": [],
    }


# Per (account, shop) serialisatie. In de huidige lijst is elke (land, shop_id)
# uniek, dus twee workers raken nooit dezelfde campagne — maar een volgend bestand
# kan dat wel bevatten, en twee gelijktijdige mutates op dezelfde boom lezen een
# stale tree en racen (verloren nodes / CONCURRENT_MODIFICATION).
_shop_locks: Dict[Tuple[str, str], threading.Lock] = {}
_shop_locks_guard = threading.Lock()


def _shop_lock(customer_id: str, shop_id: str) -> threading.Lock:
    key = (str(customer_id), str(shop_id))
    lk = _shop_locks.get(key)
    if lk is None:
        with _shop_locks_guard:
            lk = _shop_locks.get(key)
            if lk is None:
                lk = threading.Lock()
                _shop_locks[key] = lk
    return lk


def _run(rows: List[Dict[str, Any]], dry_run: bool) -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    client = _get_client()
    results: List[Dict[str, Any]] = []
    done_count = 0

    def work(row):
        with _state_lock:
            if _state["cancel"]:
                return None
        try:
            with _shop_lock(_customer_id(row["country"]), row["shop_id"]):
                return _process_row(client, row, dry_run)
        except Exception as ex:
            logger.exception("Tag Toppers rij %s mislukt", row.get("excel_row"))
            return _failed_row(row, _err(ex))

    try:
        with ThreadPoolExecutor(max_workers=RUN_WORKERS) as pool:
            futures = {pool.submit(work, r): r for r in rows}
            for fut in as_completed(futures):
                res = fut.result()
                done_count += 1
                if res is not None:
                    results.append(res)
                with _state_lock:
                    _state["current"] = done_count
                    _state["results"] = sorted(results, key=lambda r: r["excel_row"] or 0)
        results.sort(key=lambda r: r["excel_row"] or 0)
    finally:
        summary = {
            "rows": len(results),
            "campaigns_to_create": sum(1 for r in results if r["campaign_action"] == "aanmaken"),
            "ids_to_add": sum(r["ids_to_add"] for r in results),
            "ids_added": sum(r["ids_added"] for r in results),
            "exclusions_to_add": sum(r["exclusions_to_add"] for r in results),
            "exclusions_added": sum(r["exclusions_added"] for r in results),
            "negatives_copied": sum(r["negatives_copied"] for r in results),
            "errors": sum(1 for r in results if r["status"] != "ok"),
        }
        with _state_lock:
            _state["running"] = False
            _state["results"] = list(results)
            _state["summary"] = summary
            _state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")


_uploaded: Dict[str, Any] = {"rows": [], "warnings": [], "filename": None}


def set_uploaded(parsed: Dict[str, Any], filename: Optional[str] = None) -> None:
    """Bewaart de geüploade lijst zodat dry-run en echte run dezelfde rijen draaien."""
    with _state_lock:
        _uploaded["rows"] = parsed["rows"]
        _uploaded["warnings"] = parsed["warnings"]
        _uploaded["filename"] = filename


def get_uploaded() -> Dict[str, Any]:
    with _state_lock:
        return {
            "filename": _uploaded["filename"],
            "rows": len(_uploaded["rows"]),
            "warnings": list(_uploaded["warnings"]),
            "total_ids": sum(len(r["item_ids"]) for r in _uploaded["rows"]),
        }


def uploaded_rows() -> List[Dict[str, Any]]:
    with _state_lock:
        return list(_uploaded["rows"])


def start_run(rows: List[Dict[str, Any]], dry_run: bool = True) -> Dict[str, Any]:
    with _state_lock:
        if _state["running"]:
            raise RuntimeError("Er loopt al een run")
        _state.update({
            "running": True,
            "dry_run": dry_run,
            "current": 0,
            "total": len(rows),
            "cancel": False,
            "results": [],
            "summary": {},
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": None,
        })
    threading.Thread(target=_run, args=(rows, dry_run), daemon=True).start()
    return {"started": True, "total": len(rows), "dry_run": dry_run}
