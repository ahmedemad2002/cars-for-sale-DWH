import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
import os
import io

s3 = boto3.client('s3')
TODAY_STR = datetime.now(timezone.utc).strftime("%Y-%m-%d")

def extract_bronze(day_string=TODAY_STR):
    BRONZE_BUCKET_NAME = os.environ['BRONZE_BUCKET_NAME']
    BRONZE_DATA_PATH = os.environ['BRONZE_DATA_PATH']
    newest_key = BRONZE_DATA_PATH + f'/{day_string}/newest_cars.json'
    newest = s3.get_object(Bucket=BRONZE_BUCKET_NAME, Key=newest_key)
    newest = json.loads(newest["Body"].read())
    oldest_key = BRONZE_DATA_PATH + f'/{day_string}/oldest_cars.json'
    oldest = s3.get_object(Bucket=BRONZE_BUCKET_NAME, Key=oldest_key)
    oldest = json.loads(oldest["Body"].read())
    return (newest, oldest)

def deduplicate(new_cars: list, old_cars: list):
    new_cars = new_cars.get('cars')
    old_cars = old_cars.get('cars')
    new_ids = [car.get('id') for car in new_cars]
    old_cars = [car for car in old_cars if car.get('id') not in new_ids]
    new_cars.extend(old_cars)
    return new_cars

def transform(cars: list):
    # Compute all keys once, before the loop
    all_keys = set()
    for car in cars:
        for field in car.get('formattedExtraFields', []):
            all_keys.add(field['name_l1'])

    transformed_cars = []
    for car in cars:
        t_car = {}
        t_car['id'] = car['id']
        t_car['externalID'] = car['externalID']
        t_car['title'] = car['title']
        t_car['slug'] = car['slug_l1']
        t_car['description'] = car['description_l1']

        # Initialize all possible keys to None
        for key in all_keys:
            t_car.setdefault(key, None)
        # Fill in actual values
        for field in car.get('formattedExtraFields', []):
            t_car[field['name_l1']] = field['formattedValue_l1']
        # Process extra features
        if t_car.get('Extra Features') is not None:
            for feature in t_car.get('Extra Features', ["No Extra Features"]):
                t_car[f"feature_{feature}"] = True
        # delete the original list of features to avoid redundancy
        t_car.pop('Extra Features', None)
        t_car.pop('Car Category', None)
        t_car['createdAt'] = car.get('createdAt')
        t_car['updatedAt'] = car.get('updatedAt')
        t_car['_sourceURL'] = f"https://www.dubizzle.com.eg/ad/{car.get('slug_l1')}-ID{car['externalID']}.html"
        t_car['_ingestionDate'] = TODAY_STR
        transformed_cars.append(t_car)

    return transformed_cars, all_keys

def save_silver(cars: list, day_string=TODAY_STR):
    SILVER_BUCKET_NAME = os.environ['SILVER_BUCKET_NAME']

    # CSV upload
    df = pd.DataFrame(cars)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    csv_key = f'ingestion_date={day_string}/cars.csv'
    s3.put_object(
        Bucket=SILVER_BUCKET_NAME,
        Key=csv_key,
        Body=csv_buffer.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )

    # JSON upload (same data in JSON format)
    json_buffer = io.StringIO()
    json.dump(cars, json_buffer, default=str, ensure_ascii=False)

    json_key = f'ingestion_date={day_string}/cars.json'
    s3.put_object(
        Bucket=SILVER_BUCKET_NAME,
        Key=json_key,
        Body=json_buffer.getvalue().encode('utf-8'),
        ContentType='application/json'
    )

    return csv_key, json_key

def lambda_handler(event, context):
    new_cars, old_cars = extract_bronze()
    cars = deduplicate(new_cars, old_cars)
    cars, all_keys = transform(cars)
    
    csv_key, json_key = save_silver(cars)

    body = {
        "n_cars": len(cars),
        "all_keys": list(all_keys),
        'example_car': cars[0] if len(cars) > 0 else None,
        'silver_csv_key': csv_key,
        'silver_json_key': json_key
    }
    return {
        'statusCode': 200,
        'body': body,
    }