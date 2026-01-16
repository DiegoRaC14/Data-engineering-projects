import mysql.connector # For connecting to MySQL database servers
import pandas as pd # For data manipulation
import numpy as np # For numerical arrays

df = pd.read_csv(r"\Users\Usuario\Downloads\worldbank_health_economy_clean.csv")

# Change np.nan into None values so they can be recognized by MySQL
df = df.astype(object)
df = df.replace({np.nan: None})

# Connection with the table
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="[write_your_ouwn_password]",
    database="worldbank_health"
)

cursor = conn.cursor()

# Add records by using INSERT
sql = """
INSERT INTO Health_economy (
    country_code, date_year, life_expectancy, infant_mortality, death_rate,
    fertility_rate, population_total, health_expenditure_pct_gdp,
    hospital_beds_per_1000, gdp_usd, gdp_per_capita_usd,
    gdp_growth_percent, gnp_per_capita_usd, inflation_percent,
    unemployment_percent, literacy_rate,
    education_expenditure_pct_gdp, population_growth_percent,
    poverty_headcount_1_90, income_share_top_20,
    climate_disaster_impact_percent
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(sql, df.values.tolist())

# Commit changes
conn.commit()

# Close cursor and conection
cursor.close()
conn.close()

# Print information
print("Data loaded successfully!!")


