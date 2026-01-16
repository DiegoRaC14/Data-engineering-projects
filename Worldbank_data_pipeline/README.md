# World Bank Data Pipeline

This project demonstrates an **end-to-end data engineering workflow** using Python and MySQL with data from the **World Bank API**.

## Overview
- Fetch global indicators for **Mexico, USA, and Canada**
- Indicators include:
  - Population & demographics: `life_expectancy`, `infant_mortality`, `fertility_rate`
  - Health systems: `health_expenditure_pct_gdp`, `hospital_beds_per_1000`
  - Economy: `gdp_usd`, `gdp_per_capita_usd`, `gdp_growth_percent`
  - Education, social inclusion, and environment

## Pipeline Steps
1. **Extract**: Retrieve data from the World Bank API using Python
2. **Transform**: Clean, validate, and standardize columns
3. **Load**: Insert data into **MySQL** for querying
4. **Explore**: Perform basic SQL queries on the database

## Tech Stack
- Python (requests, pandas)
- MySQL / MySQL Workbench
- Jupyter Notebook (for exploration)
- SQL scripts for table creation and queries

## How to Use
1. Set up MySQL and create the `health_economy` database.
2. Run `load_to_mysql.py` to extract, clean, and insert data.
3. Query the database using SQL or Python.
4. Explore missing values, duplicates, and other quality checks in the pipeline.

## Purpose
- Shows **real-world data engineering workflow**
- Covers **API integration, cleaning, transformation, and storage**
- Demonstrates building a **reusable data pipeline** for multiple indicators and countries
