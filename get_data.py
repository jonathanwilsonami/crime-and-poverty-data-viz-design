#!/usr/bin/env python3
import os
import time
import requests
import concurrent.futures as cf
import polars as pl

# FBI Crime Data Setups
FBI_API_KEY = os.getenv("FBI_API_KEY")
if not FBI_API_KEY:
    raise SystemExit("Set FBI_API_KEY environment variable with your Data.gov key.")

YEAR_FROM = "01-2023"
YEAR_TO = "01-2024"
TARGET_YEAR_SUFFIX = "-2023" # Used to focus on only the target date

OFFENSES = ["V", "P"]
# V = "violent-crime" P = "property-crime"

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ",
    "NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA",
    "WI","WV","WY"
]

BASE = "https://api.usa.gov/crime/fbi/cde/summarized/state/{st}/{off}"

# Census Data Setups
ACS_YEAR = "2023"
ACS_DATASET = "acs/acs1"
ACS_VARS = {
    "B01003_001E": "total_population",   # Total population
    "B17001_001E": "poverty_universe",   # Poverty universe (denominator)
    "B17001_002E": "poverty_below"       # Count below poverty
}

def fetch_acs_state_poverty(year = ACS_YEAR, dataset = ACS_DATASET, var_map = ACS_VARS):
  # https://api.census.gov/data/2024/acs/acs1?get=NAME,B01003_001E,B17001_001E,B17001_002E&for=state:*
  base = f"https://api.census.gov/data/{year}/{dataset}"
  vars_param = "NAME," + ",".join(var_map.keys())
  url = f"{base}?get={vars_param}&for=state:*"

  r = requests.get(url, timeout=60)
  r.raise_for_status()
  data = r.json()

  header, *rows = data
  df = pl.DataFrame(rows, schema=header)

  rename_map = {code: name for code, name in var_map.items()}
  df = df.rename(rename_map)

  num_cols = list(rename_map.values())
  df = df.with_columns([pl.col(c).cast(pl.Float64) for c in num_cols])

  df = df.with_columns(
      (pl.col("poverty_below") / pl.col("poverty_universe") * 100.0)
      .alias("poverty_rate_pct")
  )

  acs_df = df.select(
      pl.col("NAME").alias("state_name"),
      "total_population",
      "poverty_universe",
      "poverty_below",
      "poverty_rate_pct"
  )

  return acs_df


def fbi_crime_fetch_one(state, offense, retries=3, backoff=0.8):
  params = {"from": YEAR_FROM, "to": YEAR_TO, "API_KEY": FBI_API_KEY}
  url = BASE.format(st=state, off=offense)

  for attempt in range(retries):
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        actuals = (data.get("offenses") or {}).get("actuals") or {}

        state_keys = [k for k in actuals.keys()
                      if "United States" not in k and "Clearances" not in k]
        if not state_keys:
            return {"state_abbr": state, "state_name": None,
                    "offense": offense, "total_2023": None, "error": "No state key found"}

        state_name = state_keys[0]
        months = actuals.get(state_name, {}) or {}

        # Sum the actuals. TARGET_YEAR_SUFFIX will filter on that year we want
        total_2023 = sum(v for m, v in months.items() if isinstance(v, (int, float)) and m.endswith(TARGET_YEAR_SUFFIX))

        return {
            "state_abbr": state,
            "state_name": state_name,
            "offense": offense,
            "total_2023": int(total_2023)
        }

    except requests.RequestException as e:
        if attempt == retries - 1:
            return {"state_abbr": state, "state_name": None,
                    "offense": offense, "total_2023": None, "error": str(e)}
        time.sleep(backoff * (2 ** attempt))

# Leverage parallel processing for faster results
def fbi_crime_fetch_all(states, offenses):
  jobs = [(st, off) for st in states for off in offenses]
  rows: list[dict] = []
  with cf.ThreadPoolExecutor(max_workers=12) as ex:
      futures = {ex.submit(fbi_crime_fetch_one, st, off): (st, off) for st, off in jobs}
      for fut in cf.as_completed(futures):
          rows.append(fut.result())
  return rows

import requests
import polars as pl

ACS_YEAR = "2023"
ACS_DATASET = "acs/acs1"
ACS_VARS = {
    "B01003_001E": "total_population",   # Total population
    "B17001_001E": "poverty_universe",   # Poverty universe (denominator)
    "B17001_002E": "poverty_below"       # Count below poverty
}
POVERTY_BASE = (
    "https://api.census.gov/data/{ACS_YEAR}/{ACS_DATASET}"
    "?get=NAME,B01003_001E,B17001_001E,B17001_002E&for=county:*"
)

def main():
  # crime_raw = fbi_crime_fetch_all(STATES, OFFENSES)
  # crime_df = pl.DataFrame(crime_raw)
  # 
  # acs_df = fetch_acs_state_poverty()
  # 
  # combined_data = (
  #     crime_df
  #     .join(acs_df, on="state_name", how="left")
  #     .with_columns(
  #         # compute per-100k using ACS total population
  #         (pl.col("total_2023") / pl.col("total_population") * 100_000)
  #         .alias("rate_per_100k")
  #     )
  #     .select(
  #         "state_abbr", "state_name", "offense",
  #         "total_2023",
  #         "rate_per_100k",
  #         "total_population",
  #         "poverty_universe",
  #         "poverty_below",
  #         "poverty_rate_pct"
  #     )
  # )
  # 
  # # Fix up table
  # combined_data = (
  #   combined_data.with_columns(
  #       pl.when(pl.col("offense") == "V").then(pl.lit("Violent Crime"))
  #        .when(pl.col("offense") == "P").then(pl.lit("Property Crime"))
  #        .otherwise(pl.lit("Other"))
  #        .alias("offense_label")
  #   )
  #   .sort(pl.col("offense_label").str.to_lowercase(), nulls_last=True)
  # )
  # 
  # combined_data = combined_data.rename({
  #   "total_2023": "total_crime",
  #   "rate_per_100k": "crime_rate_per_100k",
  #   "poverty_below": "below_poverty"
  # })
  # 
  # combined_data.write_csv("crime_plus_poverty_2023.csv")
  # print(f"Rows in combined_data: {combined_data.shape[0]}")
  # print(combined_data.head(8))
  
  ###########
  # Addiitonal Data clean and prep
  #####################
  crime_plus_poverty_2023 = pl.read_csv("data/crime_plus_poverty_2023.csv")

  crime_plus_poverty_2023 = (
    crime_plus_poverty_2023
    .pivot(
        index=[
            "state_abbr", "state_name",
            "total_population", "poverty_universe", "below_poverty", "poverty_rate_pct"
            # NOTE: do NOT include 'offense' or 'offense_label' here
        ],
        on="offense",                                # values are 'V' or 'P'
        values=["total_crime", "crime_rate_per_100k"],
        aggregate_function="first"                   # or "sum" if you need to sum duplicates
    )
    # Rename columns: *_V → *_violent,  *_P → *_property
    .rename({
        "total_crime_V": "total_crime_violent",
        "total_crime_P": "total_crime_property",
        "crime_rate_per_100k_V": "crime_rate_per_100k_violent",
        "crime_rate_per_100k_P": "crime_rate_per_100k_property",
    })
  )

  # (optional) sort for readability
  wide = crime_plus_poverty_2023.sort(["state_abbr", "state_name"])
  
  
  
  ######## Geocoding
  ABBR_TO_NAME = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
    "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas",
    "KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts",
    "MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana",
    "NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico",
    "NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont",
    "VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
    "DC":"District of Columbia",
  }
  
  def geocode_state_latlon(state_query: str) -> tuple[float|None,float|None]:
      """
      Use Open-Meteo Geocoding API to get (lat, lon) for a US state name.
      Returns (latitude, longitude) or (None, None) if not found.
      """
      url = "https://geocoding-api.open-meteo.com/v1/search"
      params = {"name": state_query, "country": "US", "count": 1, "language": "en"}
      try:
          r = requests.get(url, params=params, timeout=10)
          r.raise_for_status()
          data = r.json()
          results = data.get("results") or []
          if results:
              first = results[0]
              return float(first["latitude"]), float(first["longitude"])
      except Exception as e:
          print(f"[WARN] Geocode failed for {state_query}: {e}")
      return None, None
  
  # 1) Build the list of unique states to look up
  if "state_name" in wide.columns:
      state_frame = wide.select("state_abbr","state_name").unique()
  else:
      # If you don't have state_name, derive it from the abbreviation
      state_frame = wide.select("state_abbr").unique().with_columns(
          pl.col("state_abbr").map_elements(lambda ab: ABBR_TO_NAME.get(ab, ab)).alias("state_name")
      )
  
  states = list(zip(state_frame.get_column("state_abbr").to_list(),
                    state_frame.get_column("state_name").to_list()))
  
  # 2) Query API once per state (with a short pause to be polite)
  lat_map: dict[str, float] = {}
  lon_map: dict[str, float] = {}
  
  for abbr, name in states:
      lat, lon = geocode_state_latlon(name)
      # Some names (e.g., District of Columbia) sometimes resolve oddly. Fallback: try "Washington, DC".
      if lat is None or lon is None:
          if abbr == "DC":
              lat, lon = geocode_state_latlon("Washington, District of Columbia")
              if lat is None or lon is None:
                  lat, lon = geocode_state_latlon("Washington DC")
      lat_map[abbr] = lat
      lon_map[abbr] = lon
      time.sleep(0.35)  # be nice to the free API
      
  # attach state coords using the desired column names
  wide = wide.with_columns(
      pl.col("state_abbr").replace(lat_map).alias("latitude"),
      pl.col("state_abbr").replace(lon_map).alias("longitude"),
  )
  
  # sanity check for unmapped states
  missing = wide.filter(pl.any_horizontal([pl.col("latitude").is_null(), pl.col("longitude").is_null()]))
  if missing.height:
      print("Unmapped states:", missing.select("state_abbr").unique().to_series().to_list())
  
  wide.write_csv("crime_plus_poverty_2023.csv")

if __name__ == "__main__":
    main()
