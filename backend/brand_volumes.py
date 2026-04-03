"""
Brand Volumes Script

Reads brand names + IDs from an Excel file, looks up search volumes via Keyword Planner,
and saves results to an Excel file.
"""
import sys
import pandas as pd
from backend.keyword_planner_service import get_search_volumes


def run_brand_volumes(input_file: str, output_file: str):
    # Read brand names + IDs from Excel
    df_input = pd.read_excel(input_file)
    print(f"[BRAND_VOLUMES] Loaded {len(df_input)} brands from {input_file}")
    print(f"[BRAND_VOLUMES] Columns: {list(df_input.columns)}")

    brands = df_input["Name"].astype(str).tolist()

    # Look up search volumes
    result = get_search_volumes(brands)
    results = result.get("results", [])
    print(f"[BRAND_VOLUMES] Got {len(results)} results")
    print(f"[BRAND_VOLUMES] Stats: {result.get('successful', 0)} with volume, "
          f"{result.get('no_volume', 0)} no volume, {result.get('skipped', 0)} skipped, "
          f"{result.get('customer_ids_used', 0)} customer IDs used")

    # Build volume lookup: original_keyword -> search_volume
    volume_lookup = {}
    for r in results:
        volume_lookup[r["original_keyword"]] = r.get("search_volume", 0)

    # Add search_volume column to input DataFrame
    df_input["search_volume"] = df_input["Name"].astype(str).map(volume_lookup).fillna(0).astype(int)
    df_input = df_input.sort_values("search_volume", ascending=False)

    # Add grand total row
    grand_total = df_input["search_volume"].sum()
    total_row = pd.DataFrame([{"Name": "GRAND TOTAL", "Id": "", "FacetId": "", "search_volume": grand_total}])
    df_output = pd.concat([total_row, df_input], ignore_index=True)

    # Save to Excel
    df_output.to_excel(output_file, index=False, sheet_name="Brand Volumes")
    print(f"[BRAND_VOLUMES] Saved to {output_file}")
    print(f"[BRAND_VOLUMES] Grand total: {grand_total:,}")


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/app/facetvalue-inserts.xlsx"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/app/brand_volumes.xlsx"
    run_brand_volumes(input_path, output_path)
