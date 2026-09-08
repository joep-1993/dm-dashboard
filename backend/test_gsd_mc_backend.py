"""
Regression tests for the GSD Merchant Center backend (2026-09-08).

Context: Google sunset the Content API for Shopping on 2026-08-18 for GCP project
acoustic-racer-258913 and enforces it on a RANDOM SHARE of calls (~8% measured on
2026-09-08, rising to 100%). The 2026-09-08 run lost 12 of its 15 shop/country pairs to
that, in two disguises: `content_api_sunset` on the sub-account lookup, and the Google
Ads error "Resource was not found." for shops whose MC->Ads link had silently failed.

gsd_campaigns_service now prefers Merchant API v1 and falls back to the Content API.
Merchant API cannot be exercised for real yet — merchantapi.googleapis.com is still
switched off on the GCP project — so these tests pin its request/response handling with
fakes, and pin the fallback and retry behaviour around it.

What is nailed down here:
  * Merchant API listSubaccounts: pagination, and id/name extraction from either
    `accountId` or the `accounts/{id}` resource name;
  * createAndConfigure sends the account-aggregation service (without it the new account
    is NOT a sub-account of our parent and no lookup will ever see it) plus the time zone
    and language Merchant API requires, and sets the homepage URI separately;
  * a name is created EXACTLY as given — a normalised name reads as "absent" next run and
    earns the shop a second sub-account;
  * 403 SERVICE_DISABLED falls back to the Content API; any other Merchant API error
    propagates instead of quietly degrading;
  * the sunset 410 is retried and survives; a genuine 4xx is not retried;
  * the per-run listing cache pages the parent once, `refresh=True` re-reads it, and a
    just-created account is remembered (both APIs are eventually consistent);
  * link_to_google_ads asks Google Ads first (zero Merchant Center calls in the common
    case), and its return value reaches the run result — the bug that turned two silent
    linking failures into 10 doomed campaign creates.

Everything is monkeypatched — no Google API calls are made.

Run:  ./venv/bin/python -m pytest backend/test_gsd_mc_backend.py -q
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import gsd_campaigns_service as g  # noqa: E402


# --------------------------------------------------------------------------- fakes
class FakeResp:
    def __init__(self, status):
        self.status = status


class FakeHttpError(Exception):
    """Shaped like googleapiclient.errors.HttpError for the bits we branch on."""

    def __init__(self, status, message, reason=None):
        super().__init__(f"<HttpError {status} ...> {message}")
        self.resp = FakeResp(status)
        self.error_details = [{"reason": reason}] if reason else []


def sunset_error():
    return FakeHttpError(410, "Content API for Shopping was sunset on August 18, 2026",
                         reason="content_api_sunset")


def service_disabled_error():
    return FakeHttpError(403, "Merchant API has not been used in project 909970840068 "
                              "before or it is disabled.", reason="SERVICE_DISABLED")


class Call:
    """One prepared API call: .execute() returns a value or raises."""

    def __init__(self, outcome):
        self._outcome = outcome

    def execute(self):
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


class FakeAccounts:
    def __init__(self, owner):
        self.owner = owner

    def _next(self, kind, **kw):
        self.owner.calls.append((kind, kw))
        queue = self.owner.outcomes.get(kind)
        if queue is None:
            raise AssertionError(f"unexpected call: {kind}")
        return Call(queue.pop(0) if len(queue) > 1 else queue[0])

    # Merchant API
    def listSubaccounts(self, **kw):
        return self._next("listSubaccounts", **kw)

    def createAndConfigure(self, body=None):
        return self._next("createAndConfigure", body=body)

    def homepage(self):
        return self

    def updateHomepage(self, **kw):
        return self._next("updateHomepage", **kw)

    # Content API
    def list(self, **kw):
        return self._next("list", **kw)

    def insert(self, **kw):
        return self._next("insert", **kw)

    def get(self, **kw):
        return self._next("get", **kw)

    def update(self, **kw):
        return self._next("update", **kw)


class FakeService:
    """outcomes: {call_name: [result_or_exception, ...]} — the last entry repeats."""

    def __init__(self, **outcomes):
        self.outcomes = {k: list(v) for k, v in outcomes.items()}
        self.calls = []
        self._accounts = FakeAccounts(self)

    def accounts(self):
        return self._accounts

    def names(self):
        return [c[0] for c in self.calls]

    def kwargs(self, kind):
        return [c[1] for c in self.calls if c[0] == kind]


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    """Every test starts with no cached listing and an undecided backend, and can never
    reach a real API."""
    g._reset_mc_caches()
    g._merchant_service_cache.clear()
    monkeypatch.setattr(g.time, "sleep", lambda *_: None)
    monkeypatch.setattr(g, "_get_merchant_service",
                        lambda country=None: pytest.fail("merchant service not stubbed"))
    monkeypatch.setattr(g, "_get_mc_service",
                        lambda country=None: pytest.fail("content service not stubbed"))
    yield
    g._reset_mc_caches()


def use_merchant(monkeypatch, svc):
    monkeypatch.setattr(g, "_get_merchant_service", lambda country=None: svc)
    return svc


def use_content(monkeypatch, svc):
    monkeypatch.setattr(g, "_get_mc_service", lambda country=None: svc)
    return svc


def merchant_off(country=""):
    """Pin one market to the Content API, the way production looked before the three
    per-market projects were registered."""
    g._mc_state(country)["available"] = False


# --------------------------------------------------------------- Merchant API reads
def test_merchant_api_paginates_and_reads_both_id_shapes(monkeypatch):
    svc = use_merchant(monkeypatch, FakeService(listSubaccounts=[
        {"accounts": [{"name": "accounts/111", "accountId": "111", "accountName": "Alpha.nl"}],
         "nextPageToken": "p2"},
        {"accounts": [{"name": "accounts/222", "accountName": "Beta.be"}]},  # no accountId
    ]))
    out = g._list_subaccounts("5592708765")
    assert out == [{"id": "111", "name": "Alpha.nl"}, {"id": "222", "name": "Beta.be"}]
    assert svc.names() == ["listSubaccounts", "listSubaccounts"]
    first, second = svc.kwargs("listSubaccounts")
    assert first == {"provider": "accounts/5592708765", "pageSize": 500}
    assert second["pageToken"] == "p2"
    assert g._mc_backend_status()["backend"] == "merchant_api_v1"


def test_lookup_is_case_insensitive_and_caches_per_run(monkeypatch):
    svc = use_merchant(monkeypatch, FakeService(listSubaccounts=[
        {"accounts": [{"accountId": "111", "accountName": "Balmuir.com"}]},
    ]))
    assert g.get_mc_id("5592708765", "balmuir.COM") == "111"
    for _ in range(20):
        assert g.get_mc_id("5592708765", "Balmuir.com") == "111"
    assert g.get_mc_id("5592708765", "nope.nl") is None
    # 21 lookups, ONE pagination: this is the change that stops an NL shop from drawing a
    # dozen sunset-410 tickets per run.
    assert svc.names() == ["listSubaccounts"]


def test_refresh_bypasses_the_cache(monkeypatch):
    svc = use_merchant(monkeypatch, FakeService(listSubaccounts=[
        {"accounts": []},
        {"accounts": [{"accountId": "999", "accountName": "Late.nl"}]},
    ]))
    assert g.get_mc_id("5592708765", "Late.nl") is None
    assert g.get_mc_id("5592708765", "Late.nl") is None          # served from cache
    assert g.get_mc_id("5592708765", "Late.nl", refresh=True) == "999"
    assert svc.names() == ["listSubaccounts", "listSubaccounts"]


def test_first_match_wins_on_duplicate_names(monkeypatch):
    """The NL parent really does hold ~105 duplicate names; picking the LAST one would
    silently move a shop's campaigns to a different Merchant Center account."""
    use_merchant(monkeypatch, FakeService(listSubaccounts=[
        {"accounts": [{"accountId": "1", "accountName": "Dup.nl"},
                      {"accountId": "2", "accountName": "Dup.nl"}]},
    ]))
    assert g.get_mc_id("5592708765", "Dup.nl") == "1"


# --------------------------------------------------------------- Merchant API writes
def test_create_sends_aggregation_service_locale_and_homepage(monkeypatch):
    svc = use_merchant(monkeypatch, FakeService(
        createAndConfigure=[{"name": "accounts/777", "accountId": "777"}],
        updateHomepage=[{}],
    ))
    assert g._create_subaccount("5588879919", "PassaPadel|BE", "https://www.passapadel.be", "BE") == "777"

    body = svc.kwargs("createAndConfigure")[0]["body"]
    # Name verbatim — the lookup has no other key.
    assert body["account"]["accountName"] == "PassaPadel|BE"
    assert body["account"]["languageCode"] == "nl-BE"
    assert body["account"]["timeZone"] == {"id": "Europe/Brussels"}
    # Without this the account is standalone, not a sub-account of our parent.
    assert body["service"] == [{"accountAggregation": {},
                                "provider": "accounts/5588879919"}]

    hp = svc.kwargs("updateHomepage")[0]
    assert hp["name"] == "accounts/777/homepage"
    assert hp["updateMask"] == "uri"
    assert hp["body"] == {"uri": "https://www.passapadel.be"}


def test_create_survives_a_failing_homepage_update(monkeypatch):
    """A usable account with no online-store URL beats no account at all."""
    use_merchant(monkeypatch, FakeService(
        createAndConfigure=[{"accountId": "778"}],
        updateHomepage=[FakeHttpError(400, "bad uri")],
    ))
    assert g._create_subaccount("5588879919", "Shop.be", "not-a-url", "BE") == "778"


def test_unknown_country_falls_back_to_nl_locale(monkeypatch):
    svc = use_merchant(monkeypatch, FakeService(
        createAndConfigure=[{"accountId": "1"}], updateHomepage=[{}]))
    g._create_subaccount("p", "Shop.nl", "https://www.shop.nl", None)
    assert svc.kwargs("createAndConfigure")[0]["body"]["account"]["languageCode"] == "nl-NL"


def test_create_merchant_id_caches_the_new_account(monkeypatch):
    """Both APIs are eventually consistent, so the fresh account is invisible to the next
    listing for a while. Remembering it is what stops a same-run duplicate."""
    use_merchant(monkeypatch, FakeService(
        listSubaccounts=[{"accounts": []}],
        createAndConfigure=[{"accountId": "555"}],
        updateHomepage=[{}],
    ))
    assert g.get_mc_id("p", "New.nl") is None          # builds the cache
    assert g.create_merchant_id("p", "New.nl", "https://www.new.nl", "NL") == "555"
    assert g.get_mc_id("p", "New.nl") == "555"


def test_create_merchant_id_reports_the_reason_on_failure(monkeypatch):
    use_merchant(monkeypatch, FakeService(
        createAndConfigure=[FakeHttpError(400, "account name is invalid", reason="invalid")]))
    g._last_mc_error["msg"] = None
    assert g.create_merchant_id("p", "Bad|Name", "https://x.nl", "NL") is None
    assert "invalid" in (g._last_mc_error["msg"] or "")


# ------------------------------------------------------------------------- fallback
def test_service_disabled_falls_back_to_the_content_api(monkeypatch):
    merchant = use_merchant(monkeypatch, FakeService(listSubaccounts=[service_disabled_error()]))
    content = use_content(monkeypatch, FakeService(list=[
        {"resources": [{"id": 111, "name": "Alpha.nl"}]},
    ]))
    assert g._list_subaccounts("5592708765") == [{"id": "111", "name": "Alpha.nl"}]
    assert merchant.names() == ["listSubaccounts"]
    assert content.kwargs("list")[0] == {"merchantId": "5592708765", "maxResults": 250}
    assert g._mc_backend_status()["backend"] == "content_api_v2.1"

    # Within the run the market stays on the fallback: no second Merchant API attempt
    # per shop.
    g._mc_listing_cache.clear()
    g._list_subaccounts("5592708765")
    assert merchant.names() == ["listSubaccounts"]

    # A NEW run retries it, so a market that gets enabled or registered between runs is
    # picked up without restarting the backend.
    g._reset_mc_caches()
    g._list_subaccounts("5592708765")
    assert merchant.names() == ["listSubaccounts", "listSubaccounts"]


def test_creates_also_fall_back(monkeypatch):
    use_merchant(monkeypatch, FakeService(createAndConfigure=[service_disabled_error()]))
    content = use_content(monkeypatch, FakeService(insert=[{"id": 321}]))
    assert g._create_subaccount("p", "Shop.nl", "https://www.shop.nl", "NL") == "321"
    body = content.kwargs("insert")[0]["body"]
    assert body == {"name": "Shop.nl", "kind": "content#account",
                    "websiteUrl": "https://www.shop.nl"}


def test_availability_is_tracked_per_market(monkeypatch):
    """Each market authenticates with its own GCP project, so BE being unregistered says
    nothing about NL — and the run result must not claim everything migrated when one
    market is still riding the sunsetting API."""
    g._mc_state("NL")["available"] = True
    g._mc_state("BE")["available"] = False
    status = g._mc_backend_status()
    assert status["per_market"] == {"NL": "merchant_api_v1", "BE": "content_api_v2.1"}
    assert status["backend"] == "content_api_v2.1"      # worst case wins

    g._mc_state("BE")["available"] = True
    assert g._mc_backend_status()["backend"] == "merchant_api_v1"


def test_each_market_uses_its_own_key(monkeypatch):
    """One GCP project can be registered with only one Merchant Center account, so the
    NL parent must never be queried with the BE key."""
    seen = []
    monkeypatch.setattr(g, "_get_merchant_service",
                        lambda country=None: seen.append(country) or FakeService(
                            listSubaccounts=[{"accounts": []}]))
    for parent in ("5592708765", "5588879919", "5342886105"):
        g._list_subaccounts(parent)
    assert seen == ["NL", "BE", "DE"]


def test_unknown_parent_falls_back_to_the_shared_key(monkeypatch):
    seen = []
    monkeypatch.setattr(g, "_get_merchant_service",
                        lambda country=None: seen.append(country) or FakeService(
                            listSubaccounts=[{"accounts": []}]))
    g._list_subaccounts("9999999999")
    assert seen == [""]


def test_other_merchant_api_errors_are_not_swallowed(monkeypatch):
    """A 404 or a bad request must surface, not silently downgrade the whole process to
    an API Google is switching off."""
    use_merchant(monkeypatch, FakeService(listSubaccounts=[FakeHttpError(404, "no such account")]))
    with pytest.raises(FakeHttpError):
        g._list_subaccounts("nope")
    assert g._mc_state("")["available"] is None


# ---------------------------------------------------------------------- sunset 410s
def test_sunset_410_is_retried_until_it_lands(monkeypatch):
    content = use_content(monkeypatch, FakeService(list=[
        sunset_error(), sunset_error(), sunset_error(),
        {"resources": [{"id": 1, "name": "Alpha.nl"}]},
    ]))
    merchant_off()                                   # market not on Merchant API yet
    assert g._list_subaccounts("p") == [{"id": "1", "name": "Alpha.nl"}]
    assert len(content.kwargs("list")) == 4


def test_sunset_410_eventually_gives_up(monkeypatch):
    use_content(monkeypatch, FakeService(list=[sunset_error()]))
    merchant_off()
    with pytest.raises(FakeHttpError):
        g._list_subaccounts("p")


def test_sunset_410_is_classified_transient_but_a_403_is_not():
    assert g._is_sunset_error(sunset_error())
    assert g._is_transient_mc_error(sunset_error())
    assert not g._is_transient_mc_error(FakeHttpError(403, "quota", reason="quotaExceeded"))
    assert g._is_transient_mc_error(FakeHttpError(503, "backend error"))


def test_a_410_that_is_not_the_sunset_is_left_alone():
    assert not g._is_sunset_error(FakeHttpError(410, "resource is gone"))


def test_lookup_failure_never_reads_as_absent(monkeypatch):
    """(mc_id, lookup_ok) — a lookup that never completed must NOT invite a create; that
    is how Bouwlampkoning.nl and Vergewallet.nl each got two sub-accounts on 2026-09-01."""
    use_content(monkeypatch, FakeService(list=[sunset_error()]))
    merchant_off()
    mc_id, ok = g._lookup_mc_id_with_retry("p", "Shop.nl")
    assert (mc_id, ok) == (None, False)


# ----------------------------------------------------------------------- MC -> Ads
def test_link_is_a_no_op_when_google_ads_already_has_it(monkeypatch):
    """The common case must cost ZERO Merchant Center calls — it used to be an
    accounts.get, one more ticket in the sunset lottery on every shop."""
    monkeypatch.setattr(g, "_ads_product_link_exists", lambda cid, mc: True)
    monkeypatch.setattr(g, "_create_ads_product_link",
                        lambda *a: pytest.fail("should not create an existing link"))
    assert g.link_to_google_ads("parent", "5849461135", "2454295509") is True


def test_link_created_from_the_ads_side_is_verified(monkeypatch):
    seen = {"exists": False, "created": 0}

    def exists(cid, mc):
        return seen["exists"]

    def create(cid, mc):
        seen["created"] += 1
        seen["exists"] = True          # admin on both accounts: linked immediately
        return True

    monkeypatch.setattr(g, "_ads_product_link_exists", exists)
    monkeypatch.setattr(g, "_create_ads_product_link", create)
    assert g.link_to_google_ads("parent", "5849222232", "4192567576") is True
    assert seen["created"] == 1


def test_link_falls_back_to_merchant_center_when_the_ads_side_does_not_take(monkeypatch):
    monkeypatch.setattr(g, "_ads_product_link_exists", lambda cid, mc: False)
    monkeypatch.setattr(g, "_create_ads_product_link", lambda cid, mc: False)
    called = {}

    def via_mc(parent, mc_account_id, ads_customer_id, mc_id_int):
        called["hit"] = (parent, mc_account_id, ads_customer_id, mc_id_int)
        return True

    monkeypatch.setattr(g, "_link_via_merchant_center", via_mc)
    assert g.link_to_google_ads("parent", "5849222232", "4192567576") is True
    assert called["hit"] == ("parent", "5849222232", "4192567576", 5849222232)


def test_ads_side_create_that_only_files_an_invitation_still_falls_back(monkeypatch):
    """Without admin on both accounts CreateProductLink files a request Merchant Center
    must approve. The link is not live, so the MC route has to run."""
    monkeypatch.setattr(g, "_ads_product_link_exists", lambda cid, mc: False)
    monkeypatch.setattr(g, "_create_ads_product_link", lambda cid, mc: True)
    monkeypatch.setattr(g, "_link_via_merchant_center", lambda *a: True)
    assert g.link_to_google_ads("parent", "1", "2") is True


def test_unreadable_ads_link_state_does_not_report_success(monkeypatch):
    """"Could not tell" must read as "not linked" — reporting linked on a failed read is
    what puts a doomed campaign create downstream."""
    def boom(cid, mc):
        raise RuntimeError("ads query failed")

    monkeypatch.setattr(g, "_ads_product_link_exists", boom)
    monkeypatch.setattr(g, "_create_ads_product_link", lambda cid, mc: True)
    monkeypatch.setattr(g, "_link_via_merchant_center", lambda *a: False)
    assert g.link_to_google_ads("parent", "1", "2") is False


def test_ads_link_confirmed_treats_an_unreadable_answer_as_not_linked(monkeypatch):
    monkeypatch.setattr(g, "_ads_product_link_exists", lambda cid, mc: True)
    assert g._ads_link_confirmed("2454295509", 1) is True

    def boom(cid, mc):
        raise RuntimeError("ads down")

    monkeypatch.setattr(g, "_ads_product_link_exists", boom)
    assert g._ads_link_confirmed("2454295509", 1) is False


def test_merchant_center_route_writes_the_ads_link_and_accepts_the_invitation(monkeypatch):
    content = use_content(monkeypatch, FakeService(
        get=[{"id": "5849222232", "adsLinks": []}],
        update=[{}],
    ))
    monkeypatch.setattr(g, "_accept_mc_invitation", lambda cid, mc: True)
    assert g._link_via_merchant_center("5342886105", "5849222232", "4192567576", 5849222232) is True
    body = content.kwargs("update")[0]["body"]
    assert body["adsLinks"] == [{"adsId": "4192567576", "status": "active"}]


def test_merchant_center_route_does_not_claim_success_it_cannot_verify(monkeypatch):
    """MC says the link is active but no invitation shows up on the Ads side. The old code
    returned True here; that is precisely the state Joybuy.de and Balmuir.com were in."""
    use_content(monkeypatch, FakeService(
        get=[{"id": "1", "adsLinks": [{"adsId": "4192567576", "status": "active"}]}]))
    monkeypatch.setattr(g, "_accept_mc_invitation", lambda cid, mc: False)
    monkeypatch.setattr(g, "_ads_product_link_exists", lambda cid, mc: False)
    assert g._link_via_merchant_center("p", "1", "4192567576", 1) is False


def test_merchant_center_route_reports_the_reason_when_the_mc_write_fails(monkeypatch):
    use_content(monkeypatch, FakeService(get=[sunset_error()]))
    g._last_mc_error["msg"] = None
    assert g._link_via_merchant_center("p", "1", "2", 1) is False
    assert "sunset" in (g._last_mc_error["msg"] or "").lower()


# ------------------------------------------------------- the 2026-09-08 regressions
def test_get_or_create_reports_an_unlinked_account_instead_of_hiding_it(monkeypatch):
    """Joybuy.de / Balmuir.com, 2026-09-08: the MC account existed, the link silently
    failed, link_to_google_ads' False was discarded, and all 5 labels then died on the
    Google Ads error "Resource was not found." after ~2 minutes of retries each."""
    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lambda p, s, refresh=False: ("5849222232", True))
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: False)
    g._last_mc_error["msg"] = None

    mc_id, created, linked = g._get_or_create_mc_account("parent", "Joybuy.de", "4192567576", "DE")

    assert (mc_id, created, linked) == ("5849222232", False, False)
    assert "mc_ads_link_failed" in (g._last_mc_error["msg"] or "")


def test_get_or_create_is_happy_when_the_link_is_there(monkeypatch):
    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lambda p, s, refresh=False: ("5849461135", True))
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: True)
    assert g._get_or_create_mc_account("parent", "PassaPadel|BE", "2454295509", "BE") == \
        ("5849461135", False, True)


def test_get_or_create_rechecks_with_a_fresh_listing_inside_the_lock(monkeypatch):
    """The re-check inside the create lock is the duplicate guard; a cached "absent" is
    exactly the wrong answer there."""
    seen = []

    def lookup(parent, shop, refresh=False):
        seen.append(refresh)
        return (None, True) if len(seen) == 1 else ("5849460328", True)

    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lookup)
    monkeypatch.setattr(g, "_mc_account_lock", lambda p, s: _yield_true())
    monkeypatch.setattr(g, "create_merchant_id", lambda *a: pytest.fail("must reuse, not create"))
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: True)

    mc_id, created, linked = g._get_or_create_mc_account("parent", "Balmuir.com", "7938980174", "NL")
    assert (mc_id, created, linked) == ("5849460328", False, True)
    assert seen == [False, True]           # cached first, fresh inside the lock


def test_a_renamed_subaccount_is_reused_instead_of_duplicated(monkeypatch):
    """PassaPadel, 2026-09-08: sub-account 5849461135 was renamed in the Merchant Center UI
    from "PassaPadel|BE" to "PassaPadel", the next run found nothing under the feed's name
    and created a second, empty account while the five live campaigns kept pointing at the
    first. pa.mc_ids_efficy knew the right answer all along."""
    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lambda p, sh, refresh=False: (None, True))
    monkeypatch.setattr(g, "_mc_account_lock", lambda p, sh: _yield_true())
    monkeypatch.setattr(g, "current_mc_state",
                        lambda ids: {(666767, "BE"): (5849461135, "20260908", "PassaPadel|BE", 1)})
    monkeypatch.setattr(g, "_mc_account_name",
                        lambda p, mc, refresh=False: "PassaPadel")      # the new name
    monkeypatch.setattr(g, "create_merchant_id", lambda *a: pytest.fail("must not duplicate"))
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: True)

    assert g._get_or_create_mc_account("5588879919", "PassaPadel|BE", "2454295509",
                                       "BE", 666767) == ("5849461135", False, True)


def test_a_stale_state_row_does_not_send_campaigns_at_a_dead_account(monkeypatch):
    """The account the state table names is gone from the parent, so it must be ignored;
    pointing a campaign's merchant_id at a deleted account fails at create time."""
    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lambda p, sh, refresh=False: (None, True))
    monkeypatch.setattr(g, "_mc_account_lock", lambda p, sh: _yield_true())
    monkeypatch.setattr(g, "current_mc_state",
                        lambda ids: {(666767, "BE"): (111, "20260101", "Gone.be", 1)})
    monkeypatch.setattr(g, "_mc_account_name", lambda p, mc, refresh=False: None)
    monkeypatch.setattr(g, "create_merchant_id", lambda *a: "222")
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: True)

    assert g._get_or_create_mc_account("5588879919", "Shop.be", "2454295509",
                                       "BE", 666767) == ("222", True, True)


def test_state_lookup_is_skipped_without_a_shop_id(monkeypatch):
    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lambda p, sh, refresh=False: (None, True))
    monkeypatch.setattr(g, "_mc_account_lock", lambda p, sh: _yield_true())
    monkeypatch.setattr(g, "current_mc_state", lambda ids: pytest.fail("no shop_id to look up"))
    monkeypatch.setattr(g, "create_merchant_id", lambda *a: "333")
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: True)
    assert g._get_or_create_mc_account("p", "Shop.nl", "cid", "NL")[0] == "333"


def test_an_unreadable_state_table_still_lets_the_run_create(monkeypatch):
    """Best-effort guard: Redshift being down leaves us exactly where we were before it."""
    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lambda p, sh, refresh=False: (None, True))
    monkeypatch.setattr(g, "_mc_account_lock", lambda p, sh: _yield_true())
    def boom(ids): raise RuntimeError("redshift down")
    monkeypatch.setattr(g, "current_mc_state", boom)
    monkeypatch.setattr(g, "create_merchant_id", lambda *a: "444")
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: True)
    assert g._get_or_create_mc_account("p", "Shop.nl", "cid", "NL", 999)[0] == "444"


def test_state_table_is_read_once_per_shop_per_run(monkeypatch):
    calls = []
    monkeypatch.setattr(g, "current_mc_state", lambda ids: calls.append(ids) or {})
    for _ in range(5):
        assert g._mc_id_from_state("p", 666767, "BE") is None
    assert calls == [[666767]]


def test_the_id_index_tracks_names_and_new_accounts(monkeypatch):
    use_merchant(monkeypatch, FakeService(listSubaccounts=[
        {"accounts": [{"accountId": "111", "accountName": "Alpha.nl"}]},
    ]))
    assert g._mc_account_name("p", "111") == "Alpha.nl"
    assert g._mc_account_name("p", "999") is None
    g._remember_mc_account("p", "Beta.nl", "999")
    assert g._mc_account_name("p", "999") == "Beta.nl"


def test_create_inside_the_lock_passes_the_country_through(monkeypatch):
    """Merchant API needs the country for the time zone and language; forgetting it made
    every account nl-NL/Europe-Amsterdam regardless of market."""
    monkeypatch.setattr(g, "_lookup_mc_id_with_retry", lambda p, s, refresh=False: (None, True))
    monkeypatch.setattr(g, "_mc_account_lock", lambda p, s: _yield_true())
    monkeypatch.setattr(g, "link_to_google_ads", lambda *a: True)
    got = {}

    def create(parent, shop, url, country):
        got.update(parent=parent, shop=shop, url=url, country=country)
        return "5849222733"

    monkeypatch.setattr(g, "create_merchant_id", create)
    assert g._get_or_create_mc_account("5342886105", "Luhta.com|DE", "4192567576", "DE") == \
        ("5849222733", True, True)
    assert got["country"] == "DE"
    assert got["url"] == "https://www.luhta.com"


import contextlib  # noqa: E402


@contextlib.contextmanager
def _yield_true():
    yield True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
