from prefect import flow, task

@task
def extract():
    print("Extracting data...")
    return [{"item": "apple", "sales": 10}, {"item": "banana", "sales": 20}]

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
    data = extract()
    transformed = transform(data)
    load(transformed)

uk_retail_etl()
