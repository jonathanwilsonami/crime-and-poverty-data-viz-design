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
state_map = pl.DataFrame({
    "state_name": [
        "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
        "District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
        "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota",
        "Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey",
        "New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon",
        "Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
        "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming","Puerto Rico"
    ],
    "state_abbr": [
        "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
        "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR",
        "PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","PR"
    ]
})

ACS_YEAR = "2023"
ACS_DATASET = "acs/acs5"
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
    Uses wildcard for all states, with a fallback loop over state FIPS codes.
    """
    url = POVERTY_BASE.format(ACS_YEAR=ACS_YEAR, ACS_DATASET=ACS_DATASET)

    # scope query
    params = {}
    if state_fips:
        params["in"] = f"state:{state_fips}"
    else:
        params["in"] = "state:*"  # try all states in one call

    # request + fallback
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        raw = r.json()
        headers, rows = raw[0], raw[1:]
        df = pl.DataFrame(rows, schema=headers, orient="row")
    except requests.HTTPError:
        US_STATE_FIPS = [
            "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20","21","22","23",
            "24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","44",
            "45","46","47","48","49","50","51","53","54","55","56"
        ]
        parts = []
        for s in US_STATE_FIPS:
            rr = requests.get(url, params={"in": f"state:{s}"}, timeout=30)
            rr.raise_for_status()
            raw = rr.json()
            parts.append(pl.DataFrame(raw[1:], schema=raw[0], orient="row"))
        df = pl.concat(parts, how="vertical")

    # rename + cast + derived fields
    df = df.rename({**ACS_VARS, "NAME": "name"})
    df = (
        df.with_columns(
            pl.col("total_population").cast(pl.Int64, strict=False),
            pl.col("poverty_universe").cast(pl.Int64, strict=False),
            pl.col("poverty_below").cast(pl.Int64, strict=False),
            pl.col("state").cast(pl.Utf8),
            pl.col("county").cast(pl.Utf8),
        )
        .with_columns(
            pl.concat_str([pl.col("state"), pl.col("county")]).alias("fips"),
            pl.when(pl.col("poverty_universe") > 0)
              .then(pl.col("poverty_below") / pl.col("poverty_universe"))
              .otherwise(None)
              .alias("poverty_rate"),
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
  # Poverty clean up
  ###################
  # poverty_by_county_df = pl.read_csv("poverty_by_county.csv")
  # 
  # df = (
  #   poverty_by_county_df.with_columns(pl.col("name").str.split_exact(", ", 2).alias("__parts"))
  #     .with_columns([
  #         pl.col("__parts").struct.field("field_1").alias("state_name"),
  #     ])
  #     .drop("__parts")
  # )
  # 
  # df = df.join(state_map, on="state_name", how="left")
  # 
  # # 3) Drop the old FIPS parts and helper columns you said you don't need
  # to_drop = [c for c in ["state", "county", "fips"] if c in df.columns]
  # poverty_by_county_2023_final = df.drop(to_drop)
  # poverty_by_county_2023_final = poverty_by_county_2023_final.rename({"county_name_upper": "county"})
  # 
  # poverty_by_county_2023_final.write_csv("poverty_by_county_2023_final.csv")
  
  ####################
  # crime by county 
  ###################
  # agencies_2023_df = pl.read_csv("agencies_2023.csv")
  # 
  # agency_crime_2023_df = agency_crime_fetch_all_from_df(agencies_2023_df, OFFENSES)
  # agency_crime_2023_df.write_csv("crime_by_county_2023.csv")
  
  ###########
  # Clean crime by county 
  #############################
  # Dedup 
  # crime_by_county_2023_df = pl.read_csv("crime_by_county_2023.csv")
  # df = pl.read_csv("crime_by_county_2023.csv").unique(maintain_order=True)
  # 
  # # -------------------------------
  # # 1) Normalize blanks -> nulls for lat/lon
  # # -------------------------------
  # df = df.with_columns([
  #     pl.when(pl.col("latitude").cast(pl.Utf8, strict=False).str.strip_chars() == "")
  #       .then(None).otherwise(pl.col("latitude")).alias("latitude"),
  #     pl.when(pl.col("longitude").cast(pl.Utf8, strict=False).str.strip_chars() == "")
  #       .then(None).otherwise(pl.col("longitude")).alias("longitude"),
  #     pl.col("total_2023").cast(pl.Float64),
  #     pl.when(pl.col("offense") == "V").then(pl.lit("Violent Crime"))
  #    .when(pl.col("offense") == "P").then(pl.lit("Property Crime"))
  #    .otherwise(pl.col("offense"))
  #    .alias("offense")
  # ])
  # 
  # # -------------------------------
  # # 2) Split comma-separated counties
  # #    and divide total_2023 by 2 for those rows (unless zero)
  # # -------------------------------
  # has_comma = pl.col("county").cast(pl.Utf8, strict=False).str.contains(",")
  # 
  # no_split = df.filter(~has_comma)
  # 
  # split = (
  #     df.filter(has_comma)
  #       .with_columns([
  #           pl.col("county").str.split(",").alias("parts"),
  #           (pl.col("county").str.count_matches(",") + 1).alias("n_parts"),
  #       ])
  #       .explode("parts")
  #       .with_columns([
  #           pl.col("parts").str.strip_chars().alias("county"),
  #           # if total_2023 == 0 keep 0; otherwise divide by 2 (your spec),
  #           # even if there were more than 2 counties.
  #           pl.when(pl.col("total_2023") == 0)
  #             .then(pl.col("total_2023"))
  #             .otherwise(pl.col("total_2023") / pl.lit(2.0))
  #             .alias("total_2023"),
  #       ])
  #       .drop(["parts", "n_parts"])
  #       .filter(pl.col("county") != "NOT SPECIFIED")
  # )
  # 
  # df = pl.concat([no_split, split], how="vertical").unique(maintain_order=True)
  # 
  # # -------------------------------
  # # 3) Aggregate to county level
  # # -------------------------------
  # county = (
  #     df.group_by(["state_abbr", "county", "offense"])
  #       .agg([
  #           pl.col("total_2023").sum().alias("total_2023"),
  #           pl.col("latitude").drop_nulls().first().alias("latitude"),
  #           pl.col("longitude").drop_nulls().first().alias("longitude"),
  #       ])
  # )
  # 
  # # -------------------------------
  # # 4) Geocode missing lat/lon with Census (fail-fast)
  # # -------------------------------
  # def fill_latlon_with_census(
  #     df_in: pl.DataFrame,
  #     state_col: str = "state_abbr",
  #     county_col: str = "county",
  #     lat_col: str = "latitude",
  #     lon_col: str = "longitude",
  #     connect_timeout: float = 4.0,
  #     read_timeout: float = 8.0,
  #     retry: int = 1,
  #     pause: float = 0.2,  # small pause to be polite
  # ) -> pl.DataFrame:
  #     def _missing(expr: pl.Expr) -> pl.Expr:
  #         return expr.is_null() | (
  #             pl.col(expr.meta.output_name()).cast(pl.Utf8, strict=False).str.strip_chars() == ""
  #         )
  # 
  #     need = (
  #         df_in.with_row_index("_id")
  #              .filter(_missing(pl.col(lat_col)) | _missing(pl.col(lon_col)))
  #              .select("_id", state_col, county_col)
  #     )
  #     if need.is_empty():
  #         return df_in
  # 
  #     pairs = need.select(state_col, county_col).unique()
  # 
  #     def census_geocode(state_abbr: str, county_name: str):
  #         q = f"{county_name}, {state_abbr}, USA".strip(", ")
  #         url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
  #         params = {"address": q, "benchmark": "Public_AR_Current", "format": "json"}
  #         for _ in range(retry + 1):
  #             try:
  #                 r = requests.get(url, params=params, timeout=(connect_timeout, read_timeout))
  #                 if r.status_code == 200:
  #                     js = r.json()
  #                     matches = js.get("result", {}).get("addressMatches", [])
  #                     if matches:
  #                         coords = matches[0].get("coordinates", {})
  #                         # Census: x=lon, y=lat
  #                         return coords.get("y"), coords.get("x")
  #             except requests.RequestException:
  #                 pass
  #             time.sleep(pause)
  #         return None, None
  # 
  #     lat_vals, lon_vals = [], []
  #     for s, c in pairs.iter_rows():  # one call per unique (state, county)
  #         lat, lon = census_geocode(s, c)
  #         lat_vals.append(lat)
  #         lon_vals.append(lon)
  # 
  #     looked_up = pairs.with_columns([
  #         pl.Series("__lat", lat_vals),
  #         pl.Series("__lon", lon_vals),
  #     ])
  # 
  #     return (
  #         df_in.with_row_index("_id")
  #              .join(need.join(looked_up, on=[state_col, county_col], how="left"),
  #                    on="_id", how="left")
  #              .with_columns([
  #                  pl.when(_missing(pl.col(lat_col))).then(pl.col("__lat")).otherwise(pl.col(lat_col)).alias(lat_col),
  #                  pl.when(_missing(pl.col(lon_col))).then(pl.col("__lon")).otherwise(pl.col(lon_col)).alias(lon_col),
  #              ])
  #              .drop(["_id", "__lat", "__lon"])
  #     )
  # 
  # county = fill_latlon_with_census(county)
  # 
  # # -------------------------------
  # # 5) Final dedupe & quick duplicate report
  # # -------------------------------
  # county = county.unique(maintain_order=True)
  # 
  # dupes = (
  #     county.group_by(["state_abbr", "county", "offense"])
  #           .count()
  #           .filter(pl.col("count") > 1)
  #           .sort("count", descending=True)
  # )
  # 
  # print("County-level rows:", county.height)
  # print("Duplicate (state,county) pairs:", dupes.height)
  # if dupes.height:
  #     print(dupes.head(20))
  # 
  # # Final result
  # crime_by_county_2023_df_cleaned = county
  # 
  # crime_by_county_2023_df_cleaned = (
  #   crime_by_county_2023_df_cleaned.filter(
  #       pl.col("county")
  #         .cast(pl.Utf8, strict=False)
  #         .str.strip_chars()
  #         .str.to_uppercase()
  #         != "NOT SPECIFIED"
  #   )
  #   .drop(["state_abbr_right", "county_right"])  # drop the last two columns by position
  # )
  # 
  # crime_by_county_2023_df_cleaned.write_csv("crime_by_county_2023_final.csv")
  
  
  ##################################
  # Combine data
  ####################################
  crime = pl.read_csv("crime_by_county_2023_final.csv")
  poverty = pl.read_csv("poverty_by_county_2023_final.csv")

  final_df = (
      crime
      .select(["state_abbr", "county", "offense", "total_2023", "latitude", "longitude"])
      .rename({"total_2023": "total_reported_crime"})
      .join(
          poverty.select([
              "state_abbr", "county",
              "total_population", "poverty_universe", "poverty_below", "poverty_rate"
          ]),
          on=["state_abbr", "county"],
          how="left"
      )
      .with_columns(
          pl.when(pl.col("total_population") > 0)
            .then(pl.col("total_reported_crime") * pl.lit(100_000.0) / pl.col("total_population"))
            .otherwise(None)
            .alias("__per100k")
      ).with_columns([
          pl.when(pl.col("offense").is_in(["Property Crime", "P"]))
            .then(pl.col("__per100k")).otherwise(None)
            .alias("property_crime_per_100k"),
          pl.when(pl.col("offense").is_in(["Violent Crime", "V"]))
            .then(pl.col("__per100k")).otherwise(None)
            .alias("violent_crime_per_100k"),
      ]).drop(["__per100k"])
  )
  
  # Drop rows with 0 population values 
  final_df = final_df.with_columns(
    (pl.col("total_population").is_null() | (pl.col("total_population") <= 0)).alias("no_population")
  )
  
  # Have one county per row 
  wide = (
      final_df
      .select([
          "state_abbr","county","latitude","longitude","offense","total_reported_crime",
          "total_population","poverty_universe","poverty_below","poverty_rate",
          "property_crime_per_100k","violent_crime_per_100k","no_population"
      ])
      .pivot(
          index=[
              "state_abbr","county","latitude","longitude",
              "total_population","poverty_universe","poverty_below","poverty_rate","no_population"
          ],
          on="offense",
          values=["total_reported_crime","property_crime_per_100k","violent_crime_per_100k"],
          aggregate_function="first"
      )
  )

  wide.write_csv("crime_poverty_by_county_2023.csv")



