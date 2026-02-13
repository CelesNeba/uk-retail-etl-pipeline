import os
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

PROJECT_ROOT = r"C:\Users\ASUS-PC\OneDrive\Desktop\uk-retail-etl-pipeline"

RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_PATH, exist_ok=True)

# Supermarkets
supermarkets = ["Tesco", "ASDA", "Aldi", "Morrisons", "Sainsbury"]

# Products
products = [f"Product_{str(i).zfill(3)}" for i in range(1, 201)]

# Dates
start_date = datetime(2026, 1, 1)
dates = [start_date + timedelta(days=i) for i in range(90)]

# Generate rows
rows = []

for date in dates:
    for supermarket in supermarkets:
        for product in products:
            for _ in range(np.random.randint(1, 4)):
                price = round(np.random.uniform(0.5, 50), 2)

                rows.append([
                    date.strftime("%Y-%m-%d"),
                    supermarket,
                    product,
                    price
                ])

# Create DataFrame
df = pd.DataFrame(
    rows,
    columns=["raw_date", "supermarket", "product_name", "price_gbp"]
)

# Add fact id
df["fact_id"] = ["F" + str(i).zfill(6) for i in range(len(df))]

df = df[
    ["fact_id", "raw_date", "supermarket", "product_name", "price_gbp"]
]

# Save CSV
file_path = os.path.join(RAW_PATH, "retail_sample_data.csv")

df.to_csv(file_path, index=False)

print("Raw data saved to:", file_path)
