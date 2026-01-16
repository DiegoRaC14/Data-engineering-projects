# %% [markdown]
# # **World Bank API**
# 
# <div align="justify">
# The World Bank API provides access to files, databases, and metadata related to global indicators such as population statistics, health systems, economic performance, education, social inclusion, and environmental factors across different countries.
# 
# For this pipeline, data was retrieved from several indicators of previous fields for three North American countries: Canada, the United States, and Mexico. The data was then merged and cleaned to ensure a consistent structure across all indicators. As part of the process, the dataset was briefly analyzed to identify missing values and overall data quality.
# 
# Finally, the cleaned dataset was stored in MySQL using MySQL Workbench. This exercise represents a complete data engineering workflow, covering data extraction, transformation, validation, storage, and movement between systems.
# </div>
# 

# %%
import matplotlib.pyplot as plt # Visualizations
import pandas as pd # Dataframes
import requests # API connection

# %%
# Define relevant variables to consider
countries = {
    'MEX': 'Mexico',
    'CAN': 'Canada',
    'USA': 'United States of America'
  }


# Select indicators
indicators = {
    
  # Population (SP)
  "SP.DYN.LE00.IN": "life_expectancy",
  "SP.DYN.IMRT.IN": "infant_mortality",
  "SP.DYN.CDRT.IN": "death_rate",
  "SP.DYN.TFRT.IN": "fertility_rate",
  "SP.POP.TOTL": "population_total",

  # Health systems (SH)
  "SH.XPD.CHEX.GD.ZS": "health_expenditure_pct_gdp",
  "SH.MED.BEDS.ZS": "hospital_beds_per_1000",

  # Economy (NY)
  "NY.GDP.MKTP.CD": "gdp_usd",
  "NY.GDP.PCAP.CD": "gdp_per_capita_usd",
  "NY.GDP.MKTP.KD.ZG": "gdp_growth_percent",
  "NY.GNP.PCAP.CD": "gnp_per_capita_usd",
  "NY.INC.PCAP.CD": "gni_per_capita_usd",
  "NY.EXP.GNFS.ZS": "exports_percent_gdp",
  "NY.IMP.GNFS.ZS": "imports_percent_gdp",
  "FP.CPI.TOTL.ZG": "inflation_percent",
  "SL.UEM.TOTL.ZS": "unemployment_percent",

  # Education (SE)
  "SE.ADT.LITR.ZS": "literacy_rate",
  "SE.XPD.TOTL.GD.ZS": "education_expenditure_pct_gdp",

  # Social inclusion / Poverty (SI)
  "SP.POP.TOTL": "population_total",
  "SP.POP.GROW": "population_growth_percent",
  "SI.POV.DDAY": "poverty_headcount_1_90",
  "SI.DST.04TH.20": "income_share_top_20",

  # Environment (EN)
  "EN.CLC.MDAT.ZS": "climate_disaster_impact_percent",
}


# %%
# Function to fetch a specific country with a specific indicator
def fetch_indicator(country_code, ind_code, ind_name):

    params = {"format": "json", 
              "page": 1}

    # URL 
    url = f"https://api.worldbank.org/v2/countries/{country_code}/indicators/{ind_code}"

    response = requests.get(url, params=params)

    # Obtain data
    data = response.json()

    # Check if data was returned by the API (not empty)

    # Data is an array with dictionaries, the first element is the metadata and the 
    # second is the data itself
    if len(data) < 2 or data[1] is None:
        return pd.DataFrame()

    df = pd.DataFrame(data[1]) # Convert into a dataframe
    df = df[["countryiso3code", "date", "value"]] # Select columns

    # Change column names
    df.columns = ["country_code", "year", ind_name]

    return df


# %%
# Fetch all indicators for a single country
def retrieve_data_country(country_code):
    base_df = pd.DataFrame()

    # Iterate over the indicator codes and names
    for ind_code, ind_name in indicators.items():

        # Checking if data is loading (debbuging)
        # print(f"Fetching {ind_name}")

        ind_df = fetch_indicator(country_code, ind_code, ind_name)

        # Skip if no data was returned
        if ind_df.empty:
            continue

        # Initialize dataframe with the first indicator
        if base_df.empty:
            base_df = ind_df
            continue

        # Add column to the left only if it does not exist
        if ind_name not in base_df.columns:
            base_df = base_df.merge(
                ind_df,
                on=["country_code", "year"],
                how="left"
            )

    return base_df


# %%
# Build a unified dataset for all selected countries
def merged_database(countries):

    # Empty list to storage 
    dfs_array = []

    for country_code in countries.keys():
        dfs_array.append(retrieve_data_country(country_code))

    # Combine all countries into a single df
    final_database = pd.concat(dfs_array, ignore_index = True)

    return final_database
        

health_df = merged_database(countries)
health_df


# %%
# Cleaning and validatiing step for the pipeline after exploring the dataset
def clean_validate_dataset(df):

    df = df.copy()

    # Standardize column names
    df.columns = df.columns.str.lower()

    # Ensure year is numeric
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Replace empty strings with NaN
    df = df.replace("", pd.NA)

    # Convert numeric columns (except identifiers)
    for col in df.columns:
        if col not in ["country_code"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows without key identifiers
    before_rows = len(df)
    df = df.dropna(subset=["country_code", "year"])
    after_rows = len(df)

    print(f"Dropped rows with missing keys: {before_rows - after_rows} ")

    df = df.sort_values(["country_code", "year"]) # Sort for consistency (and better-looking)

    df = df.reset_index(drop=True) # Reset index after cleaning and sorting, so it doesn't look weird
    
    df = df.rename(columns = {'year': 'date_year'})

    
    ### ---- Print data quality checks ----

    # Check duplicate country-year records
    duplicates = df.duplicated(subset=["country_code", "date_year"]).sum()
    print(f"Duplicate country-year rows: {duplicates}\n")

    # Check missing values per column
    missing_summary = df.isna().sum()
    print("\nMissing values per column:\n")
    print(missing_summary[missing_summary > 0])


    return df


# %%
final_df = clean_validate_dataset(health_df)

# %%
final_df

# %%
# Save final clean dataset to CSV
output_path = "worldbank_health_economy_clean.csv"

final_df.to_csv(output_path,index=False)

# Print info
print(f"Clean dataset saved to {output_path}!")


