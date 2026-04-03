import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
import os
import io

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

def invoke_next(function_name: str, payload: dict):
    """Fire-and-forget async invoke of the next Lambda in the chain."""
    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',          # async — ScrapeDay doesn't wait
        Payload=json.dumps(payload).encode()
    )
    print(f"  → Invoked {function_name} asynchronously")


def extract_bronze(day_string):
    BRONZE_BUCKET_NAME = os.environ['BRONZE_BUCKET_NAME']
    BRONZE_DATA_PATH = os.environ['BRONZE_DATA_PATH']
    newest_key = BRONZE_DATA_PATH + f'/{day_string}/newest_cars.json'
    newest = s3.get_object(Bucket=BRONZE_BUCKET_NAME, Key=newest_key)
    newest = json.loads(newest["Body"].read())
    oldest_key = BRONZE_DATA_PATH + f'/{day_string}/oldest_cars.json'
    oldest = s3.get_object(Bucket=BRONZE_BUCKET_NAME, Key=oldest_key)
    oldest = json.loads(oldest["Body"].read())
    return (newest, oldest)

def merge_cars(new_cars: list, old_cars: list):
    new_cars = new_cars.get('cars')
    old_cars = old_cars.get('cars')
    new_ids = [car.get('id') for car in new_cars]
    old_cars = [car for car in old_cars if car.get('id') not in new_ids]
    new_cars.extend(old_cars)
    return new_cars

def transform(cars: list, TODAY_STR: str):
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

def change_dtypes(cars: list) -> pd.DataFrame:
    for car in cars:
        if car.get('createdAt') is not None:
            car['createdAt'] = datetime.fromtimestamp(car['createdAt'], tz=timezone.utc).isoformat()
        if car.get('updatedAt') is not None:
            car['updatedAt'] = datetime.fromtimestamp(car['updatedAt'], tz=timezone.utc).isoformat()
        car['Price'] = int(car['Price'].replace(',', '').split('.')[0]) if car.get('Price') is not None else None

    df = pd.DataFrame(cars)
    df.drop_duplicates(subset=['id'], inplace=True)
    feature_cols = [c for c in df.columns if c.startswith('feature_')]
    for col in feature_cols:
        df[col] = df[col].astype('boolean')

    numeric_cols = [
        'Year', 'Price', 'Kilometers', 'Down Payment',
        'Power (hp)', 'Engine Capacity (CC)',
        'Consumption (l/100 km)', 'Number of seats', 'Number of Owners',
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')

    return df


def save_silver(df: pd.DataFrame, cars: list, day_string):
    SILVER_BUCKET_NAME = os.environ['SILVER_BUCKET_NAME']

    # Parquet — from typed df
    parquet_buffer = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, parquet_buffer, compression='snappy')
    parquet_buffer.seek(0)
    parquet_key = f'ingestion_date={day_string}/cars.parquet'
    s3.put_object(
        Bucket=SILVER_BUCKET_NAME,
        Key=parquet_key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/octet-stream'
    )

    return parquet_key


def lambda_handler(event, context):
    # ── Resolve date range from event ─────────────────────────────────────────
    # Normal daily run:   event = {}  → processes today only
    # Backfill:           event = {"start_date": "2026-03-23", "end_date": "2026-03-26"}
    # Single day:         event = {"start_date": "2026-03-23", "end_date": "2026-03-23"}

    today = datetime.now(timezone.utc).date()

    start_date = datetime.strptime(
        event.get('start_date', str(today)), "%Y-%m-%d"
    ).date()
    end_date = datetime.strptime(
        event.get('end_date', str(today)), "%Y-%m-%d"
    ).date()

    if start_date > end_date:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    # ── Build list of dates to process ───────────────────────────────────────
    from datetime import timedelta
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(str(current))
        current += timedelta(days=1)

    print(f"Processing {len(dates)} day(s): {dates[0]} → {dates[-1]}")

    # ── Process each day ─────────────────────────────────────────────────────
    results = []
    for day_string in dates:
        print(f"  Processing {day_string} ...")
        try:
            new_cars, old_cars = extract_bronze(day_string)
            cars = merge_cars(new_cars, old_cars)
            cars, all_keys = transform(cars, day_string)
            df = change_dtypes(cars)
            parquet_key = save_silver(df, cars, day_string)

            results.append({
                'date': day_string,
                'status': 'success',
                'n_cars': len(cars),
                'parquet_key': parquet_key,
            })
            print(f"  ✓ {day_string} — {len(cars)} cars")

        except Exception as e:
            # Don't stop the whole backfill if one day fails
            results.append({
                'date': day_string,
                'status': 'failed',
                'error': str(e),
            })
            print(f"  ✗ {day_string} — {str(e)}")

# ── Sanity check before handing off to Gold ───────────────────────────────
    successful_days = [r for r in results if r['status'] == 'success']
    failed_days     = [r for r in results if r['status'] == 'failed']

    ROW_THRESHOLD = int(os.environ.get('MIN_SILVER_ROWS', '500'))
    thin_days = [r for r in successful_days if r['n_cars'] < ROW_THRESHOLD]

    if failed_days or thin_days:
        print(f"  ⚠ Skipping Gold invocation — "
              f"{len(failed_days)} failed day(s), "
              f"{len(thin_days)} day(s) below {ROW_THRESHOLD}-row threshold")
    else:
        s2g_function = os.environ.get('SILVER_TO_GOLD_FUNCTION_NAME')
        if s2g_function:
            # Pass the same date range forward so Gold processes the same window
            gold_payload = {
                'start_date': dates[0],
                'end_date':   dates[-1],
            }
            invoke_next(s2g_function, gold_payload)
        else:
            print("  ⚠ SILVER_TO_GOLD_FUNCTION_NAME not set — skipping chain")

    return {
        'statusCode': 200,
        'body': {
            'processed': len(successful_days),
            'failed':    len(failed_days),
            'results':   results,
        }
    }