CREATE DATABASE IF NOT EXISTS worldbank_health;
USE worldbank_health;

-- DROP TABLE IF EXISTS Health_economy;

-- Create table with all indicators
CREATE TABLE Health_economy(
    country_code VARCHAR(3) NOT NULL,
    date_year INT NOT NULL,

    life_expectancy FLOAT,
    infant_mortality FLOAT,
    death_rate FLOAT,
    fertility_rate FLOAT,

    population_total BIGINT,

    health_expenditure_pct_gdp FLOAT,
    hospital_beds_per_1000 FLOAT,

    gdp_usd DOUBLE,
    gdp_per_capita_usd DOUBLE,
    gdp_growth_percent FLOAT,
    gnp_per_capita_usd DOUBLE,

    inflation_percent FLOAT,
    unemployment_percent FLOAT,
    literacy_rate FLOAT,
    education_expenditure_pct_gdp FLOAT,

    population_growth_percent FLOAT,
    poverty_headcount_1_90 FLOAT,
    income_share_top_20 FLOAT,
    climate_disaster_impact_percent FLOAT,

    -- Composite primary key (country + year)
    PRIMARY KEY (country_code, date_year)
);

-- Count how many records were loaded
SELECT COUNT(*) 
FROM Health_economy; -- 150 -> that is correct!!

-- Show original database
SELECT * 
FROM Health_economy;

-- Countries with highest GDP per capita 
SELECT country_code, date_year, gdp_per_capita_usd
FROM Health_economy
ORDER BY gdp_per_capita_usd DESC
LIMIT 5;

-- Life expectancy trend for Mexico
SELECT date_year, life_expectancy
FROM Health_economy
WHERE country_code = 'MEX'
ORDER BY date_year;


-- Relationship between health spending and life expectancy in Canada
SELECT country_code, date_year,
       health_expenditure_pct_gdp,
       life_expectancy
FROM Health_economy
WHERE health_expenditure_pct_gdp IS NOT NULL
AND life_expectancy IS NOT NULL AND country_code = 'CAN';





