import pandas as pd
import mysql.connector
import os

# -----------------------------
# 1. Connect to MySQL staging (no password)
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    database="staging"
)

# -----------------------------
# 2. Load staging table
# -----------------------------
query = "SELECT * FROM stg_retail_prices"
df = pd.read_sql(query, conn)

print("Initial number of rows:", len(df))

# -----------------------------
# 3. Check for NULLs
# -----------------------------
null_summary = df.isnull().sum()
print("\nNULL values per column:\n", null_summary)

# Drop rows with any NULLs
df = df.dropna()
print("Rows after dropping NULLs:", len(df))

# -----------------------------
# 4. Remove invalid prices
# -----------------------------
df = df[df["price_gbp"] > 0]
print("Rows after removing invalid prices:", len(df))

# -----------------------------
# 5. Drop duplicates
# -----------------------------
df = df.drop_duplicates()
print("Rows after dropping duplicates:", len(df))

# -----------------------------
# 6. Price distribution
# -----------------------------
min_price = df["price_gbp"].min()
max_price = df["price_gbp"].max()
avg_price = df["price_gbp"].mean()
std_price = df["price_gbp"].std()

print("\nPrice distribution:")
print(f"Min: {min_price}, Max: {max_price}, Mean: {avg_price:.2f}, Std: {std_price:.2f}")

# -----------------------------
# 7. Detect outliers using IQR
# -----------------------------
Q1 = df["price_gbp"].quantile(0.25)
Q3 = df["price_gbp"].quantile(0.75)
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

outliers = df[(df["price_gbp"] < lower_bound) | (df["price_gbp"] > upper_bound)]
print(f"Number of outliers detected: {len(outliers)}")

# Optionally, remove outliers
df = df[(df["price_gbp"] >= lower_bound) & (df["price_gbp"] <= upper_bound)]
print("Rows after removing outliers:", len(df))

# -----------------------------
# 8. Save clean CSV
# -----------------------------
processed_path = r"C:\Users\ASUS-PC\OneDrive\Desktop\uk-retail-etl-pipeline\data\processed\retail_clean.csv"
os.makedirs(os.path.dirname(processed_path), exist_ok=True)

df.to_csv(processed_path, index=False)
print("\nClean CSV saved to:", processed_path)

# -----------------------------
# 9. Close connection
# -----------------------------
conn.close()
