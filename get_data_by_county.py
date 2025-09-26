import os
import time
import requests
import concurrent.futures as cf
import polars as pl

# ----------------------------
# Crime Data Setups
# ----------------------------
FBI_API_KEY = os.getenv("FBI_API_KEY")
if not FBI_API_KEY:
    raise SystemExit("Set FBI_API_KEY environment variable with your Data.gov key.")

YEAR_FROM = "01-2023"
YEAR_TO = "01-2024"
TARGET_YEAR_SUFFIX = "-2023"

OFFENSES = ["V", "P"]
STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ",
    "NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA",
    "WI","WV","WY"
]

# STATES = [
#     "AL"
# ]

# https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/VA?API_KEY=iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv # Agency
# https://api.usa.gov/crime/fbi/cde/summarized/agency/VA0010100/V?from=01-2024&to=01-2025&API_KEY=iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv # Summarized

ORI_BASE = "https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/{st}"
SUMMARIZED_OBI_BASE = "https://api.usa.gov/crime/fbi/cde/summarized/agency/{st}/{off}"

# ----------------------------
# Poverty Data Setups
# ----------------------------
ACS_YEAR = "2023"
ACS_DATASET = "acs/acs1"
ACS_VARS = {
    "B01003_001E": "total_population",   # Total population
    "B17001_001E": "poverty_universe",   # Poverty universe (denominator)
    "B17001_002E": "poverty_below"       # Count below poverty
}
POVERTY_BASE = "https://api.census.gov/data/{ACS_YEAR}/{ACS_DATASET}?get=NAME,B01003_001E,B17001_001E,B17001_002E&for=county:*"

def fbi_agencies_fetch_one(state, retries=3, backoff=0.8):
    """
    Fetch agencies for one state from ORI_BASE, which returns:
      { "LEE": [ {...agency...}, ... ],
        "BATH": [ {...}, ... ],
        ... }
    Returns {"state_abbr": <state>, "error": <str|None>, "rows": [normalized dicts]}.
    """
    params = {"API_KEY": FBI_API_KEY}
    url = ORI_BASE.format(st=state)

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            rows = []

            # Accept a few shapes; most commonly it's a dict keyed by county
            if isinstance(data, dict):
                county_dict = data.get("results", data)  # sometimes nested under "results"
                if not isinstance(county_dict, dict):
                    return {"state_abbr": state, "error": "Unexpected response shape", "rows": []}

                for county_key, agencies in county_dict.items():
                    if not isinstance(agencies, list):
                        continue
                    county_from_key = (str(county_key).strip().upper() if county_key is not None else "UNKNOWN")

                    for a in agencies:
                        ori = a.get("ori")
                        if not ori:
                            continue

                        # Prefer the item field if present, else fall back to county key
                        county_val = (
                            a.get("counties")
                            or a.get("county_name")
                            or a.get("county")
                            or county_from_key
                            or "UNKNOWN"
                        )
                        county_norm = str(county_val).strip().upper()

                        rows.append({
                            "ori": ori,
                            "county": county_norm,
                            "state_abbr": a.get("state_abbr") or a.get("state_code") or state,
                            "state_name": a.get("state_name"),
                            "agency_name": a.get("agency_name"),
                            "agency_type_name": a.get("agency_type_name"),
                            "is_nibrs": bool(a.get("is_nibrs")),
                            "nibrs_start_date": a.get("nibrs_start_date"),
                            "latitude": a.get("latitude"),
                            "longitude": a.get("longitude"),
                        })

                return {"state_abbr": state, "error": None, "rows": rows}

            elif isinstance(data, list):
                # Fallback: some endpoints return a flat list
                for a in data:
                    ori = a.get("ori")
                    if not ori:
                        continue
                    county_norm = str(
                        a.get("counties") or a.get("county_name") or a.get("county") or "UNKNOWN"
                    ).strip().upper()
                    rows.append({
                        "ori": ori,
                        "county": county_norm,
                        "state_abbr": a.get("state_abbr") or a.get("state_code") or state,
                        "state_name": a.get("state_name"),
                        "agency_name": a.get("agency_name"),
                        "agency_type_name": a.get("agency_type_name"),
                        "is_nibrs": bool(a.get("is_nibrs")),
                        "nibrs_start_date": a.get("nibrs_start_date"),
                        "latitude": a.get("latitude"),
                        "longitude": a.get("longitude"),
                    })
                return {"state_abbr": state, "error": None, "rows": rows}

            # Unknown shape
            return {"state_abbr": state, "error": "Unexpected response shape", "rows": []}

        except requests.RequestException as e:
            if attempt == retries - 1:
                return {"state_abbr": state, "error": str(e), "rows": []}
            time.sleep(backoff * (2 ** attempt))


def fbi_agencies_fetch_all(states):
    rows = []
    with cf.ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(fbi_agencies_fetch_one, st): st for st in states}
        for fut in cf.as_completed(futs):
            res = fut.result()
            if res.get("error"):
                print(f"[ERR] {res['state_abbr']}: {res['error']}")
            rows.extend(res.get("rows", []))
    return rows
  
# Crime data by ORI and OFFENSE 
import re  # <-- add this with your other imports

def agency_crime_fetch_one(ori: str, offense: str, retries: int = 3, backoff: float = 0.8):
    """Fetch summarized monthly actuals for a single ORI/offense and sum the target year."""
    if offense not in ("V", "P"):
        return {"ori": ori, "offense": offense, f"total_{YEAR_FROM.split('-')[1]}": None,
                "error": "offense must be 'V' or 'P'"}

    params = {"from": YEAR_FROM, "to": YEAR_TO, "API_KEY": FBI_API_KEY}
    url = SUMMARIZED_OBI_BASE.format(st=ori, off=offense)

    target_year = int(YEAR_FROM.split("-")[1])
    total_field = f"total_{target_year}"

    def _to_int(x):
        try:
            if isinstance(x, int):
                return x
            if isinstance(x, float):
                return int(x)
            if isinstance(x, str) and x.strip():
                return int(float(x))
        except Exception:
            return None
        return None

    def _key_has_year(k: str, year: int) -> bool:
        if not isinstance(k, str):
            return False
        s = k.strip()
        return s.endswith(f"-{year}") or s.startswith(f"{year}-")

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            # ===== NEW SHAPE: {"offenses": {"actuals": {"<agency_name>": {"MM-YYYY": value, ...}}}} =====
            if isinstance(data, dict):
              offenses = data.get("offenses")
              # print(f"offenses: {offenses}")
              if isinstance(offenses, dict):
                  actuals = offenses.get("actuals")
                  # print(f"actuals: {actuals}")
                  if isinstance(actuals, dict) and actuals:
                      # Prefer exact key match; otherwise pick the first key that is NOT a "clearances" series
                      keys = list(actuals.keys())
          
                      # 1) exact match on provided ori (if the API ever uses the same string)
                      agency_key = ori if ori in actuals else None
          
                      # 2) otherwise prefer a key that does NOT include "clearance"/"clearances"
                      if agency_key is None:
                          non_clearance_keys = [k for k in keys
                                                if isinstance(k, str) and "clearance" not in k.lower()]
                          if non_clearance_keys:
                              agency_key = non_clearance_keys[0]
          
                      # 3) last resort: just take the first key
                      if agency_key is None:
                          agency_key = keys[0]
          
                      month_map = actuals.get(agency_key, {})
                      # print(f"month_map: {month_map}")
                      if isinstance(month_map, dict):
                          total = 0
                          for mk, mv in month_map.items():
                              if _key_has_year(mk, target_year):
                                  val = _to_int(mv)
                                  if val is not None:
                                      total += val
                          return {"ori": ori, "offense": offense, total_field: int(total)}

            # ===== LEGACY SHAPE: {"results": [...]} or bare list =====
            results = data.get("results", data if isinstance(data, list) else [])
            if not isinstance(results, list):
                return {"ori": ori, "offense": offense, total_field: None, "error": "Unexpected response shape"}

            total = 0
            for item in results:
                if not isinstance(item, dict):
                    continue

                # value
                val = item.get("actual")
                if val is None:
                    for k in ("offense_count", "count", "value"):
                        if item.get(k) is not None:
                            val = item[k]
                            break
                val = _to_int(val)
                if val is None:
                    continue

                # explicit year or month key
                yr = item.get("data_year", item.get("year"))
                if isinstance(yr, str):
                    import re
                    m = re.search(r"\d{4}", yr)
                    yr = int(m.group(0)) if m else None

                mo = item.get("month") or item.get("date") or item.get("data_month")
                in_target = (isinstance(yr, int) and yr == target_year)
                if not in_target and isinstance(mo, str):
                    mo = mo.strip()
                    if mo.startswith(f"{target_year}-") or mo.endswith(f"-{target_year}"):
                        in_target = True

                if in_target:
                    total += val

            return {"ori": ori, "offense": offense, total_field: int(total)}

        except requests.RequestException as e:
            if attempt == retries - 1:
                return {"ori": ori, "offense": offense, total_field: None, "error": str(e)}
            time.sleep(backoff * (2 ** attempt))



def agency_crime_fetch_all_from_df(agencies_2023_df: pl.DataFrame, offenses: list[str], max_workers: int = 12) -> pl.DataFrame:
    """Iterate ORIs from agencies_2023_df × offenses (V/P). Returns a Polars DataFrame."""
    if "ori" not in agencies_2023_df.columns:
        return pl.DataFrame()

    # unique, non-empty ORIs
    oris = (
        agencies_2023_df.get_column("ori")
        .drop_nulls()
        .unique()
        .to_list()
    )
    oris = [o for o in oris if str(o).strip()]

    jobs = [(ori, off) for ori in oris for off in offenses]
    rows: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(agency_crime_fetch_one, ori, off): (ori, off) for ori, off in jobs}
        # print(f"futs: {futs}")
        for fut in cf.as_completed(futs):
          # print(f"result fut: {fut.result()}")
          rows.append(fut.result())

    out = pl.from_dicts(rows) if rows else pl.DataFrame()
    if out.is_empty():
        return out

    # Join minimal context back from agencies_2023_df (dedupe by ORI)
    meta_cols = [c for c in ("ori","state_abbr","county","agency_name","agency_type_name","latitude","longitude")
                 if c in agencies_2023_df.columns]
    if meta_cols:
        meta = agencies_2023_df.select(meta_cols).unique(subset=["ori"])
        out = out.join(meta, on="ori", how="left")

    return out


def fetch_acs_poverty(state_fips: str | None = None) -> pl.DataFrame:
    """
    Download ACS poverty data (county level) as Polars DataFrame.
    If state_fips is provided (e.g., '01' for AL), restricts to that state.
    """
    url = POVERTY_BASE.format(ACS_YEAR=ACS_YEAR, ACS_DATASET=ACS_DATASET)
    params = {}
    if state_fips:
        params["in"] = f"state:{state_fips}"

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    raw = r.json()  # list of lists

    headers = raw[0]
    rows = raw[1:]

    # Build DF directly from rows + headers
    df = pl.DataFrame(rows, schema=headers)

    # Rename columns to friendly names using your ACS_VARS
    df = df.rename({**ACS_VARS, "NAME": "name"})

    # Cast numeric columns and add convenience fields
    df = (
        df
        .with_columns(
            pl.col("total_population").cast(pl.Int64, strict=False),
            pl.col("poverty_universe").cast(pl.Int64, strict=False),
            pl.col("poverty_below").cast(pl.Int64, strict=False),
            pl.col("state").cast(pl.Utf8),
            pl.col("county").cast(pl.Utf8),
        )
        .with_columns(
            # 5-digit county FIPS (already zero-padded in API response)
            pl.concat_str([pl.col("state"), pl.col("county")]).alias("fips"),
            # Safe poverty rate
            pl.when(pl.col("poverty_universe") > 0)
              .then(pl.col("poverty_below") / pl.col("poverty_universe"))
              .otherwise(None)
              .alias("poverty_rate"),
            # Optional: uppercase county name without " County" for joins
            pl.col("name")
              .str.split_exact(", ", 2)
              .struct.field("field_0")
              .str.replace(" County", "")
              .str.to_uppercase()
              .alias("county_name_upper"),
        )
    )

    return df

if __name__ == "__main__":
  ####################
  # Agencies
  ###################
  # agency_rows = fbi_agencies_fetch_all(STATES)
  # agencies_df = pl.from_dicts(agency_rows)
  # agencies_df.write_csv("agencies_2023.csv")
  
  ####################
  # Poverty by county
  ###################
  # all_counties = fetch_acs_poverty()
  # all_counties.head(3)
  # all_counties.write_csv("poverty_by_county.csv")
  
  ####################
  # crime by county 
  ###################
  agencies_2023_df = pl.read_csv("agencies_2023.csv")
  
  agency_crime_2023_df = agency_crime_fetch_all_from_df(agencies_2023_df, OFFENSES)
  agency_crime_2023_df.write_csv("agency_crime_2023.csv")

  # keep_states = ["AL"]
  # agencies_3state_df = agencies_2023_df.filter(
  #     pl.col("state_abbr").is_in(keep_states)
  # )
  # #   
  # summ_df = agency_crime_fetch_all_from_df(agencies_3state_df, OFFENSES)
  # print(summ_df.head(4))
  # summ_df.write_csv("test.csv")


# # ----------------------------
# # Census ACS fetch
# # ----------------------------
# def fetch_acs_state_poverty(year: str = ACS_YEAR,
#                             dataset: str = ACS_DATASET,
#                             var_map: dict[str, str] = ACS_VARS) -> pl.DataFrame:
#     """
#     Fetch ACS 1-year state totals for population + poverty, all states.
#     Returns Polars DF with: state_name, state_fips, total_population, poverty_universe,
#     poverty_below, poverty_rate_pct.
#     """
#     base = f"https://api.census.gov/data/{year}/{dataset}"
#     vars_param = "NAME," + ",".join(var_map.keys())
#     url = f"{base}?get={vars_param}&for=state:*"
#     if CENSUS_API_KEY:
#         url += f"&key={CENSUS_API_KEY}"
# 
#     r = requests.get(url, timeout=60)
#     r.raise_for_status()
#     data = r.json()  # first row header
# 
#     header, *rows = data
#     df = pl.DataFrame(rows, schema=header)
# 
#     # Rename coded vars to readable names
#     rename_map = {code: name for code, name in var_map.items()}
#     df = df.rename(rename_map)
# 
#     # Cast numeric + compute poverty rate (% of poverty universe)
#     num_cols = list(rename_map.values())
#     df = df.with_columns([pl.col(c).cast(pl.Float64) for c in num_cols])
#     df = df.with_columns(
#         (pl.col("poverty_below") / pl.col("poverty_universe") * 100.0)
#         .alias("poverty_rate_pct")
#     )
# 
#     acs_df = df.select(
#         pl.col("NAME").alias("state_name"),
#         pl.col("state").alias("state_fips"),
#         "total_population",
#         "poverty_universe",
#         "poverty_below",
#         "poverty_rate_pct"
#     )
#     return acs_df
# 
# # ----------------------------
# # FBI CDE fetch (per state/offense)
# # ----------------------------
# def fbi_crime_fetch_one(state: str, offense: str, retries: int = 3, backoff: float = 0.8) -> dict:
#     """
#     Call the CDE summarized/state endpoint with offense tokens (V or P).
#     Returns a dict: state_abbr, state_name, offense, total_2024 (monthly sum).
#     """
#     params = {"from": YEAR_FROM, "to": YEAR_TO, "API_KEY": FBI_API_KEY}
#     url = FBI_BASE.format(st=state, off=offense)
# 
#     for attempt in range(retries):
#         try:
#             r = requests.get(url, params=params, timeout=30)
#             r.raise_for_status()
#             data = r.json()
# 
#             # Navigate new CDE shape:
#             # data["offenses"]["actuals"] is a dict with keys like "Alabama", "United States", "Alabama Clearances"
#             actuals = (data.get("offenses") or {}).get("actuals") or {}
# 
#             # Find the key for this state (exclude US and Clearances)
#             state_keys = [k for k in actuals.keys() if "United States" not in k and "Clearances" not in k]
#             if not state_keys:
#                 return {
#                     "state_abbr": state, "state_name": None, "offense": offense,
#                     "total_2024": None, "error": "No state key found"
#                 }
# 
#             state_name = state_keys[0]
#             months = actuals.get(state_name, {}) or {}
# 
#             total_2024 = sum(
#                 v for m, v in months.items()
#                 if isinstance(v, (int, float)) and m.endswith(TARGET_YEAR_SUFFIX)
#             )
# 
#             return {
#                 "state_abbr": state,
#                 "state_name": state_name,
#                 "offense": offense,
#                 "total_2024": int(total_2024)
#             }
# 
#         except requests.RequestException as e:
#             if attempt == retries - 1:
#                 return {
#                     "state_abbr": state, "state_name": None, "offense": offense,
#                     "total_2024": None, "error": str(e)
#                 }
#             time.sleep(backoff * (2 ** attempt))
# 
# # Parallel fan-out to all states × offenses
# def fbi_crime_fetch_all(states: list[str], offenses: list[str]) -> list[dict]:
#     jobs = [(st, off) for st in states for off in offenses]
#     rows: list[dict] = []
#     with cf.ThreadPoolExecutor(max_workers=12) as ex:
#         futures = {ex.submit(fbi_crime_fetch_one, st, off): (st, off) for st, off in jobs}
#         for fut in cf.as_completed(futures):
#             rows.append(fut.result())
#     return rows
# 
# # ----------------------------
# # Main
# # ----------------------------
# def main():
#     # FBI crime (state x offense)
#     crime_raw = fbi_crime_fetch_all(STATES, OFFENSES)
#     crime_df = pl.DataFrame(crime_raw)
# 
#     # Census ACS population & poverty (one row per state)
#     acs_df = fetch_acs_state_poverty()
# 
#     # Join and compute rates
#     combined_data = (
#         crime_df
#         .join(acs_df, on="state_name", how="left")
#         .with_columns(
#             # avoid divide-by-zero/nulls
#             pl.when(pl.col("total_population") > 0)
#               .then((pl.col("total_2024") / pl.col("total_population")) * 100_000)
#               .otherwise(None)
#               .alias("crime_rate_per_100k"),
#             # human-friendly offense label
#             pl.when(pl.col("offense") == "V").then(pl.lit("Violent Crime"))
#              .when(pl.col("offense") == "P").then(pl.lit("Property Crime"))
#              .otherwise(pl.lit("Other"))
#              .alias("offense_label")
#         )
#         .rename({
#             "total_2024": "total_crime",
#             "poverty_below": "below_poverty"
#         })
#         .select(
#             "state_abbr", "state_name", "state_fips",
#             "offense", "offense_label",
#             "total_crime", "crime_rate_per_100k",
#             "total_population", "poverty_universe", "below_poverty", "poverty_rate_pct"
#         )
#         .sort(["state_abbr", "offense_label"])
#     )
# 
#     # Write output
#     out_csv = "state_crime_poverty_2024_long.csv"
#     combined_data.write_csv(out_csv)
#     print(f"Wrote {out_csv} with {combined_data.shape[0]} rows")
#     print(combined_data.head(10))
# 
#     # Optional: also produce a wide table with V/P columns
#     wide = (
#         combined_data
#         .select(
#             "state_abbr", "state_name", "state_fips",
#             "offense", "total_crime", "crime_rate_per_100k"
#         )
#         .pivot(
#             values=["total_crime", "crime_rate_per_100k"],
#             index=["state_abbr", "state_name", "state_fips"],
#             columns="offense",
#             aggregate_function="first"
#         )
#         .rename({
#             "total_crime_V": "violent_total_2024",
#             "crime_rate_per_100k_V": "violent_rate_per_100k",
#             "total_crime_P": "property_total_2024",
#             "crime_rate_per_100k_P": "property_rate_per_100k",
#         })
#         .sort(["state_abbr"])
#     )
#     out_wide = "state_crime_poverty_2024_wide.csv"
#     wide.write_csv(out_wide)
#     print(f"Wrote {out_wide} with {wide.shape[0]} rows")
#     print(wide.head(10))
# 
# if __name__ == "__main__":
#     main()
