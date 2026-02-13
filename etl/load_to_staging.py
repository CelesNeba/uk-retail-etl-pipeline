import pandas as pd
import mysql.connector

# -----------------------------
# 1. Connect to MySQL (no password)
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    database="staging"
)

# -----------------------------
# 2. Path to raw CSV
# -----------------------------
csv_path = r"C:\Users\ASUS-PC\OneDrive\Desktop\uk-retail-etl-pipeline\data\raw\retail_sample_data.csv"

# -----------------------------
# 3. Load CSV into pandas
# -----------------------------
df = pd.read_csv(csv_path)

# -----------------------------
# 4. Write to MySQL staging table
# -----------------------------
# Convert DataFrame to tuples for MySQL insert
cols = ", ".join(df.columns)
placeholders = ", ".join(["%s"] * len(df.columns))
sql_insert = f"REPLACE INTO stg_retail_prices ({cols}) VALUES ({placeholders})"

cursor = conn.cursor()

# Insert row by row
for row in df.itertuples(index=False):
    cursor.execute(sql_insert, row)

conn.commit()
cursor.close()
conn.close()

print("Loaded into staging database successfully!")
