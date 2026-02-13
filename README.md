# UK Supermarket retail ETL and analytics pipeline

## Problem statement

Retail supermarkets in the UK generate vast amounts of pricing and sales data daily, but struggle to monitor pricing trends, track competitor products, and identify inflationary patterns because sales and pricing data are scattered across multiple supermarkets in inconsistent formats and enormous volumes. Without a structured workflow, manually consolidating this information is time-consuming, error-prone, and delays business decisions. An ETL pipeline is essential to ***Extract*** data from disparate sources, ***Transform*** it into clean, standardized tables, and ***Load*** it into an analytics-ready database, enabling timely, accurate insights and strategic decision-making.


## Business questions to answer

1. Which supermarkets have the highest average prices for products?

2. How do prices for the same product vary across different supermarkets?

3. Are there noticeable price trends or inflation patterns over the 90-day period?

3. Which products are the best-selling or most frequently priced?

4. Are there pricing outliers that could indicate errors, promotions, or anomalies?


## Project Goal

- Build a scalable and **reproducible ETL pipeline** for UK retail data.  
- Generate clean, standardized, and queryable datasets.  
- Create supporting **dimension and fact tables** in MySQL.  
- Enable advanced analytics and **interactive Power BI dashboards**.  
- Demonstrate best practices in retail data engineering for portfolio presentation.




## Expected outcomes

- **Clean, standardized dataset** for UK retail prices  
- **Robust dimension and fact tables** supporting analytics    
- **Interactive Power BI dashboards** for business insights  
- **Reproducible, portfolio-ready ETL project** demonstrating best practices  


## How to run

1. Clone the repository:

```bash
git clone https://github.com/CelesNeba/uk-retail-etl-pipeline.git
cd uk-retail-etl-pipeline

2. Install Python dependencies
 - pip install -r requirements.txt
3. Run ETL scripts in order
python .\etl\extract_generate_data.py
python .\etl\transform_clean.py
python .\etl\load_to_staging.py
python .\etl\load_to_analytics.py

4. ***Explore the project files*** to review the ETL workflow and understand the code logic behind each stage 






## Tools & technologies used

| Layer | Tools |
|-------|-------|
| Data Processing & ETL | Python, Jupyter Notebook, pandas, SQLAlchemy |
| Database | MySQL (Staging + Analytics) |
| Visualization | Power BI |
| Version Control | GitHub |



## Dataset: (generated data)

> ### Reason for using generated data:  

The publicly available Kaggle dataset had over 90 million rows, which MySQL could not handle on a local setup (it kept crashing). By generating a **smaller but realistic dataset**, we retain the same structure and information while making it manageable for ETL development and testing.

- **Supermarkets:** Tesco, ASDA, Aldi, Morrisons, Sainsbury  
- **Products:** 200 synthetic products  
- **Timeframe:** 90 days  
- **Output:** CSV files in `data/raw` and `data/processed`



## ETL pipeline

This project follows the **classic ETL workflow**:

### **1️⃣ Extract**
- Generate synthetic retail data (`extract_generate_data.py`)  
- Save raw data as CSV in the `data/raw` folder  
- Structure: `fact_id`, `raw_date`, `supermarket`, `product_name`, `price_gbp`  

### **2️⃣ Transform**
- Load raw CSV into staging database (`load_to_staging.py`)  
- Perform **data cleaning** in `transform_clean.py`:
  - Remove **NULLs**  
  - Remove invalid prices  
  - Drop duplicates  
  - Detect and remove **outliers** (IQR method)  
- Save clean CSV to `data/processed`  

- Create **dimension tables** in analytics database:
  - `dim_supermarket`  
  - `dim_product`  
  - `dim_date`
    

### **3️⃣ Load**
- Load transformed data into **analytics schema** (`load_to_analytics.py`)  
- Build **fact table** and **aggregated tables** for analysis  
- Optimized for querying in Power BI dashboards  



## Project structure in folders

uk-retail-etl-pipeline/

│
├── etl/ # ETL scripts

│ ├── extract_generate_data.py

│ ├── load_to_staging.py

│ ├── transform_clean.py

│ └── load_to_analytics.py

│
├── data/ # Datasets

│ ├── raw/ # Raw CSV files
│ └── processed/ # Cleaned CSV files
│

├── sql/ # SQL scripts for staging & analytics
│

├── notebooks/ # Jupyter notebooks for analysis & testing
│

├── dashboards/ # Power BI dashboards

│ └── Power BI dashboard.pbix
│

├── README.md # Project overview and instructions

├── requirements.txt # Python dependencies

└── .gitignore # Git ignore rules








