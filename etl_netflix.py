# =============================================================================
# ETL Mini-Project: Netflix Movies and TV Shows
# CPSC 5071-02 | Data Management for Data Science
# =============================================================================

import pandas as pd
import sqlite3
import os

# =============================================================================
# STEP 1: EXTRACT
# =============================================================================

print("=" * 60)
print("STEP 1: EXTRACT")
print("=" * 60)

# Load the raw dataset
df = pd.read_csv("netflix_titles.csv")

# Initial inspection
print(f"\nDataset loaded successfully.")
print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns\n")

print("First 5 rows:")
print(df.head())

print("\nDataset info:")
print(df.info())

print("\nMissing values per column:")
print(df.isnull().sum())


# =============================================================================
# STEP 2: TRANSFORM
# =============================================================================

print("\n" + "=" * 60)
print("STEP 2: TRANSFORM")
print("=" * 60)

# --- 2a. Clean column names ---
# Standardize to lowercase with underscores
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_", regex=False)
    .str.replace(r"[^\w]", "_", regex=True)
)
print("\nCleaned column names:", df.columns.tolist())


# --- 2b. Drop rows missing required fields ---
rows_before = len(df)
df.dropna(subset=["title", "release_year"], inplace=True)
print(f"\nRows dropped (missing title or release_year): {rows_before - len(df)}")
print(f"Rows remaining: {len(df)}")


# --- 2c. Fill non-critical missing values with 'Unknown' ---
# These are useful for analysis but not required for every record
for col in ["director", "cast", "country", "rating"]:
    df[col] = df[col].fillna("Unknown")

# date_added: drop remaining nulls (only ~10 rows) since we need it for derived column
rows_before = len(df)
df.dropna(subset=["date_added"], inplace=True)
print(f"Rows dropped (missing date_added): {rows_before - len(df)}")


# --- 2d. Convert date_added to datetime ---
df["date_added"] = pd.to_datetime(df["date_added"].str.strip(), format="%B %d, %Y")
print(f"\ndate_added dtype after conversion: {df['date_added'].dtype}")


# --- 2e. Create derived column: year_added ---
df["year_added"] = df["date_added"].dt.year
print(f"year_added column created. Sample values: {df['year_added'].value_counts().head(3).to_dict()}")


# --- 2f. Normalize the type column ---
df["type"] = df["type"].str.strip().str.title()


# --- 2g. (Optional) Split listed_in into primary_genre ---
# listed_in contains comma-separated genres; we extract the first listed genre
# as a clean "primary_genre" column for easier querying
df["primary_genre"] = df["listed_in"].str.split(",").str[0].str.strip()
print(f"\nprimary_genre column created from listed_in.")
print("Sample genres:", df["primary_genre"].value_counts().head(5).to_dict())


# --- 2h. Standardize duration into numeric + unit columns ---
# Separates "90 min" into duration_value=90 and duration_unit="min"
df["duration_value"] = df["duration"].str.extract(r"(\d+)").astype("float")
df["duration_unit"] = df["duration"].str.extract(r"([a-zA-Z]+)")
print(f"\nduration split into duration_value and duration_unit.")


# --- Final inspection ---
print(f"\nFinal dataset shape: {df.shape}")
print(f"\nFinal columns: {df.columns.tolist()}")
print(f"\nNull check after cleaning:\n{df.isnull().sum()}")


# =============================================================================
# STEP 3: LOAD
# =============================================================================

print("\n" + "=" * 60)
print("STEP 3: LOAD")
print("=" * 60)

db_path = "netflix_cleaned.db"

# Remove existing DB if re-running
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)

# Load cleaned dataframe into SQLite
df.to_sql("netflix_shows", conn, if_exists="replace", index=False)

print(f"\nData loaded into SQLite database: {db_path}")
print(f"Table created: netflix_shows")

# Confirm row count via SQL
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM netflix_shows")
row_count = cursor.fetchone()[0]
print(f"Rows in database: {row_count}")


# =============================================================================
# STEP 4: VALIDATE WITH SQL QUERIES
# =============================================================================

print("\n" + "=" * 60)
print("STEP 4: SQL VALIDATION QUERIES")
print("=" * 60)


def run_query(label, query):
    print(f"\n--- {label} ---")
    result = pd.read_sql_query(query, conn)
    print(result.to_string(index=False))
    return result


# Query 1: Count Movies vs TV Shows
run_query(
    "Query 1: Count of Movies vs TV Shows",
    """
    SELECT type, COUNT(*) AS total
    FROM netflix_shows
    GROUP BY type
    ORDER BY total DESC
    """
)

# Query 2: Top 10 countries producing the most titles
run_query(
    "Query 2: Top 10 Countries by Title Count",
    """
    SELECT country, COUNT(*) AS total
    FROM netflix_shows
    WHERE country != 'Unknown'
    GROUP BY country
    ORDER BY total DESC
    LIMIT 10
    """
)

# Query 3: Titles added to Netflix per year
run_query(
    "Query 3: Titles Added Per Year",
    """
    SELECT year_added, COUNT(*) AS titles_added
    FROM netflix_shows
    GROUP BY year_added
    ORDER BY year_added DESC
    """
)

# Query 4: Average release year for Movies vs TV Shows
run_query(
    "Query 4: Average Release Year by Type",
    """
    SELECT type, ROUND(AVG(release_year), 2) AS avg_release_year
    FROM netflix_shows
    GROUP BY type
    """
)

# Query 5: Top 10 primary genres across all content
run_query(
    "Query 5: Top 10 Primary Genres",
    """
    SELECT primary_genre, COUNT(*) AS total
    FROM netflix_shows
    GROUP BY primary_genre
    ORDER BY total DESC
    LIMIT 10
    """
)

# Query 6: Movies with the longest average duration by genre
run_query(
    "Query 6: Top 5 Genres with Longest Avg Movie Duration (in minutes)",
    """
    SELECT primary_genre, ROUND(AVG(duration_value), 1) AS avg_duration_min
    FROM netflix_shows
    WHERE type = 'Movie' AND duration_unit = 'min'
    GROUP BY primary_genre
    ORDER BY avg_duration_min DESC
    LIMIT 5
    """
)


conn.close()
print("\n" + "=" * 60)
print("ETL PIPELINE COMPLETE")
print(f"Database saved to: {db_path}")
print("=" * 60)
