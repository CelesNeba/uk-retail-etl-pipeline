import pandas as pd
import mysql.connector
import os

# -----------------------------
# 1️⃣ Connect to MySQL analytics DB (no password)
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    database="analytics"
)
cursor = conn.cursor()

# -----------------------------
# 2️⃣ Load the clean CSV
# -----------------------------
csv_path = r"C:\Users\ASUS-PC\OneDrive\Desktop\uk-retail-etl-pipeline\data\processed\retail_clean.csv"
df = pd.read_csv(csv_path)

print("Clean CSV rows to load:", len(df))

# -----------------------------
# 3️⃣ Ensure dimension tables exist
# -----------------------------
# Supermarket dimension
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_supermarket (
    supermarket_id VARCHAR(10) PRIMARY KEY,
    supermarket_name VARCHAR(100),
    location VARCHAR(100),
    store_type VARCHAR(50)
)
""")

# Insert static supermarket data (skip if already exists)
supermarkets = [
    ('T01','Tesco','London','Superstore'),
    ('A01','ASDA','Birmingham','Superstore'),
    ('AL01','Aldi','Manchester','Discount'),
    ('M01','Morrisons','Leeds','Superstore'),
    ('S01','Sainsbury','Bristol','Superstore')
]

cursor.executemany("""
INSERT IGNORE INTO dim_supermarket 
(supermarket_id, supermarket_name, location, store_type)
VALUES (%s, %s, %s, %s)
""", supermarkets)

# Map supermarket_name -> supermarket_id
supermarket_map = {name: sid for sid, name, loc, stype in supermarkets}

# Product dimension
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(150)
)
""")

# Insert products if not exists
product_tuples = [(f"P{str(i+1).zfill(3)}", p) for i, p in enumerate(df["product_name"].unique())]
cursor.executemany("""
INSERT IGNORE INTO dim_product (product_id, product_name) VALUES (%s, %s)
""", product_tuples)

# Map product_name -> product_id
product_map = {p[1]: p[0] for p in product_tuples}

# Date dimension
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(15)
)
""")

dates_tuples = [(pd.to_datetime(d).date(),
                 pd.to_datetime(d).day,
                 pd.to_datetime(d).month,
                 pd.to_datetime(d).year,
                 pd.to_datetime(d).strftime("%A"))
                for d in df["raw_date"].unique()]

cursor.executemany("""
INSERT IGNORE INTO dim_date (date_id, day, month, year, weekday) VALUES (%s, %s, %s, %s, %s)
""", dates_tuples)

# Commit dimension inserts
conn.commit()
print("Dimension tables updated.")

# -----------------------------
# 4️⃣ Create fact table
# -----------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS fact_prices (
    fact_id VARCHAR(32) PRIMARY KEY,
    date_id DATE,
    product_id VARCHAR(10),
    supermarket_id VARCHAR(10),
    price_gbp DECIMAL(10,2)
)
""")
conn.commit()

# -----------------------------
# 5️⃣ Load fact table
# -----------------------------
fact_tuples = []
for row in df.itertuples(index=False):
    fact_tuples.append((
        row.fact_id,
        pd.to_datetime(row.raw_date).date(),
        product_map[row.product_name],
        supermarket_map[row.supermarket],
        row.price_gbp
    ))

insert_sql = """
REPLACE INTO fact_prices (fact_id, date_id, product_id, supermarket_id, price_gbp)
VALUES (%s, %s, %s, %s, %s)
"""

cursor.executemany(insert_sql, fact_tuples)
conn.commit()

print("Fact table loaded. Total rows:", len(fact_tuples))

# -----------------------------
# 6️⃣ Close connection
# -----------------------------
cursor.close()
conn.close()
print("Analytics load complete!")
