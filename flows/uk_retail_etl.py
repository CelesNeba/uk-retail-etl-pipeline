from prefect import flow, task

@task
def extract():
    print("Extracting data...")
    data = [{"item": "apple", "sales": 10}, {"item": "banana", "sales": 20}]
    return data

@task
def transform(data):
    print("Transforming data...")
    for row in data:
        row["sales"] *= 2
    return data

@task
def load(data):
    print("Loading data...")
    for row in data:
        print(row)

@flow
def uk_retail_etl():
    raw = extract()
    clean = transform(raw)
    load(clean)

if __name__ == "__main__":
    uk_retail_etl()
