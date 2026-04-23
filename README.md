# Netflix ETL Pipeline
**Course:** CPSC 5071 - Data Management for Data Science  
**Institution:** Seattle University, MS in Data Science  
**Term:** Winter 2026

---

## Overview

A full Extract-Transform-Load pipeline built in Python that cleans and loads the Netflix Movies and TV Shows dataset into a queryable SQLite database, then runs SQL analysis to surface content trends.

The raw dataset has 8,807 rows and 12 columns with significant missing data - especially in `director` (2,634 nulls), `cast`, and `country`. The pipeline handles all of that systematically before loading into a structured database.

---

## Pipeline Structure

### Step 1 - Extract
- Loads `netflix_titles.csv` into a pandas DataFrame
- Inspects shape, dtypes, and null counts before any transformation

### Step 2 - Transform
- Standardizes column names (lowercase, underscores)
- Drops rows missing `title`, `release_year`, or `date_added`
- Fills non-critical nulls (`director`, `cast`, `country`, `rating`) with `"Unknown"` to preserve otherwise valid records
- Converts `date_added` from string to datetime (with `.str.strip()` to catch silent whitespace failures)
- Derives `year_added` from `date_added`
- Normalizes `type` column casing
- Splits `listed_in` into a `primary_genre` column for cleaner querying
- Parses `duration` into separate `duration_value` (numeric) and `duration_unit` columns

### Step 3 - Load
- Saves cleaned DataFrame into a SQLite database (`netflix_cleaned.db`)
- Table name: `netflix_shows`

### Step 4 - Validate with SQL
Six queries run against the loaded database:

| Query | What it answers |
|-------|----------------|
| Movies vs TV Shows count | Content type breakdown |
| Top 10 countries by title count | Where Netflix content comes from |
| Titles added per year | Growth and licensing trends over time |
| Average release year by type | How old the movie vs TV catalog is |
| Top 10 primary genres | What Netflix mostly carries |
| Longest avg movie duration by genre | Which genres run longest |

---

## Key Findings

- The US leads with ~2,800 titles; India came in at nearly 1,000 - larger than the UK, Japan, and South Korea combined
- Movies average a release year of 2013 vs 2016 for TV Shows - the movie catalog leans on older licensed content
- Content additions peaked in 2019–2020, then slowed - reflecting Netflix's shift away from volume licensing toward originals

---

## How to Run

```bash
python etl_netflix.py
```

Requires `netflix_titles.csv` in the same directory. Generates `netflix_cleaned.db` as output.

**Dependencies:** Python 3, pandas, sqlite3 (stdlib)

---

## Tools & Technologies

- **Language:** Python 3
- **Libraries:** `pandas`, `sqlite3`
- **Storage:** SQLite

---

## Files

| File | Description |
|------|-------------|
| `etl_netflix.py` | Full ETL pipeline script |
| `netflix_titles.csv` | Raw dataset (source: Kaggle) |
| `netflix_cleaned.db` | Cleaned SQLite database output |
| `Reflection.txt` | Written reflection on design decisions and findings |

---

## About

Part of the MS in Data Science program at Seattle University, covering data engineering concepts including ETL pipeline design, missing value strategy, datetime parsing, feature engineering, and SQL validation.
