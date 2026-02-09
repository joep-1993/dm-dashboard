"""
Keyword Planner Service

Queries Google Ads Keyword Planner API for search volumes.
Normalizes keywords for API lookup while preserving original keywords for output.
Rotates through multiple customer_ids to handle quota limits.
"""
import os
import re
import time
from typing import List, Dict, Optional
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Customer IDs for quota rotation
CUSTOMER_IDS = [
    '8485842412', '4056770576', '1496704472', '4964513580', '3114657125', '5807833423', '3273661472',
    '7269160392', '9251309631', '3969307564', '8273243429', '8696777335', '5930401821', '6213822688',
    '6379322129', '2237802672', '8338942127', '9525057729', '8431844135', '6862783922', '6511658729',
    '4675585929', '5105960927', '4567815835', '1351439239', '5122292229', '7346695290', '5550062935',
    '4761604080', '6044293584', '6271552035', '8755979133', '7938980174', '8276523186', '4192567576'
]

# API settings
GEO_TARGET = "2528"       # Netherlands
LANGUAGE = "1010"         # Dutch
BATCH_SIZE = 10000
MAX_RETRY_ATTEMPTS = 5


def _get_client() -> GoogleAdsClient:
    """Initialize Google Ads client from environment variables."""
    config = {
        "developer_token": os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
        "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        "login_customer_id": os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", ""),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def clean_keyword(keyword: str) -> str:
    """Normalize keyword for API lookup: replace - and _ with spaces, remove special chars, lowercase."""
    keyword = re.sub(r'[-_]', ' ', keyword)
    keyword = re.sub(r'[^a-zA-Z0-9\s]', '', keyword)
    keyword = ' '.join(keyword.split())
    return keyword.lower()


def validate_keyword(keyword: str) -> bool:
    """Validate keyword: max 80 chars, only alphanumeric and spaces."""
    if not keyword or len(keyword) > 80:
        return False
    if re.search(r'[^a-zA-Z0-9\s]', keyword):
        return False
    return True


def _query_search_volumes(client: GoogleAdsClient, keywords: List[str], customer_id: str) -> Optional[Dict[str, int]]:
    """
    Query Google Ads Keyword Planner API for search volumes.
    Returns dict mapping keyword -> avg_monthly_searches, or None if quota exhausted.
    """
    if not keywords:
        return {}

    googleads_service = client.get_service("GoogleAdsService")
    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

    request = client.get_type("GenerateKeywordHistoricalMetricsRequest")
    request.customer_id = customer_id
    request.keywords.extend(keywords)
    request.geo_target_constants.append(googleads_service.geo_target_constant_path(GEO_TARGET))
    request.keyword_plan_network = client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
    request.language = googleads_service.language_constant_path(LANGUAGE)

    attempt = 0
    while attempt < MAX_RETRY_ATTEMPTS:
        try:
            response = keyword_plan_idea_service.generate_keyword_historical_metrics(request=request)
            results = {}
            for result in response.results:
                metrics = result.keyword_metrics
                results[result.text] = metrics.avg_monthly_searches
            return results
        except GoogleAdsException as ex:
            error_details = "\n".join([f"{error.error_code}: {error.message}" for error in ex.failure.errors])
            print(f"[KEYWORD_PLANNER] API error for customer {customer_id}: {error_details}")
            if ex.error.code().name == "RESOURCE_EXHAUSTED":
                attempt += 1
                if attempt >= MAX_RETRY_ATTEMPTS:
                    print(f"[KEYWORD_PLANNER] Quota exhausted for customer_id {customer_id}")
                    return None  # Signal quota exhaustion
                wait_time = min(2 ** attempt * 10, 600)
                print(f"[KEYWORD_PLANNER] Quota exceeded, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                raise

    return None


def get_search_volumes(keywords: List[str]) -> Dict:
    """
    Get search volumes for a list of keywords.
    Normalizes keywords for API lookup, maps results back to originals.

    Args:
        keywords: List of original keywords (e.g., ["e-bike", "hardloopschoenen"])

    Returns:
        Dict with results list and summary stats
    """
    if not keywords:
        return {"results": [], "total": 0, "successful": 0, "failed": 0}

    client = _get_client()

    # Build mapping: cleaned_keyword -> list of original keywords
    cleaned_to_originals: Dict[str, List[str]] = {}
    skipped = []

    for original in keywords:
        original = original.strip()
        if not original:
            continue
        cleaned = clean_keyword(original)
        if not validate_keyword(cleaned):
            skipped.append({"keyword": original, "reason": f"Invalid after cleaning: '{cleaned}'"})
            continue
        if cleaned not in cleaned_to_originals:
            cleaned_to_originals[cleaned] = []
        cleaned_to_originals[cleaned].append(original)

    # Deduplicated list of cleaned keywords to query
    unique_cleaned = list(cleaned_to_originals.keys())
    print(f"[KEYWORD_PLANNER] {len(keywords)} input keywords -> {len(unique_cleaned)} unique normalized keywords")

    # Query API in batches with customer_id rotation
    all_volumes: Dict[str, int] = {}
    customer_id_index = 0

    for batch_start in range(0, len(unique_cleaned), BATCH_SIZE):
        if customer_id_index >= len(CUSTOMER_IDS):
            print("[KEYWORD_PLANNER] All customer_ids exhausted")
            break

        batch = unique_cleaned[batch_start:batch_start + BATCH_SIZE]
        batch_num = (batch_start // BATCH_SIZE) + 1
        total_batches = (len(unique_cleaned) + BATCH_SIZE - 1) // BATCH_SIZE

        while customer_id_index < len(CUSTOMER_IDS):
            customer_id = CUSTOMER_IDS[customer_id_index]
            print(f"[KEYWORD_PLANNER] Batch {batch_num}/{total_batches}: {len(batch)} keywords, customer {customer_id}")

            result = _query_search_volumes(client, batch, customer_id)
            if result is None:
                # Quota exhausted, try next customer
                customer_id_index += 1
                continue

            all_volumes.update(result)
            print(f"[KEYWORD_PLANNER] Batch {batch_num} complete: {len(result)} results")
            break
        else:
            print(f"[KEYWORD_PLANNER] All customer_ids exhausted at batch {batch_num}")
            break

    # Map results back to original keywords
    # Also try matching by removing all spaces (fallback from original script)
    results = []
    for cleaned, originals in cleaned_to_originals.items():
        volume = all_volumes.get(cleaned)
        if volume is None:
            # Fallback: try without spaces
            no_spaces = re.sub(r'\s+', '', cleaned)
            volume = all_volumes.get(no_spaces)

        for original in originals:
            results.append({
                "original_keyword": original,
                "normalized_keyword": cleaned,
                "search_volume": volume if volume is not None else 0
            })

    # Add skipped keywords
    for skip in skipped:
        results.append({
            "original_keyword": skip["keyword"],
            "normalized_keyword": "",
            "search_volume": 0,
            "error": skip["reason"]
        })

    return {
        "results": results,
        "total": len(results),
        "successful": sum(1 for r in results if r.get("search_volume", 0) > 0),
        "no_volume": sum(1 for r in results if r.get("search_volume", 0) == 0 and "error" not in r),
        "skipped": len(skipped),
        "unique_keywords_queried": len(unique_cleaned),
        "customer_ids_used": customer_id_index + 1
    }


def test_api_connection() -> Dict:
    """Test the Google Ads Keyword Planner API connection with a single keyword."""
    try:
        client = _get_client()
        customer_id = CUSTOMER_IDS[0]

        result = _query_search_volumes(client, ["test"], customer_id)
        if result is not None:
            return {
                "status": "success",
                "message": f"API connection successful. Test keyword 'test' returned volume: {result.get('test', 'N/A')}",
                "customer_id": customer_id
            }
        else:
            return {
                "status": "error",
                "message": "API returned None (quota may be exhausted for first customer_id)"
            }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
