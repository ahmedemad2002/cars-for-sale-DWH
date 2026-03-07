import json
import boto3
import requests
from datetime import datetime, timezone
import os

# ── Config ────────────────────────────────────────────────────────────────────
s3 = boto3.client('s3')
def load_config() -> dict:
    bucket = os.environ["BUCKET_NAME"]
    key = f"config.json"
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())
CONFIG = load_config()

S3_BUCKET = CONFIG.get('s3').get('bucket')
S3_PREFIX = CONFIG.get('s3').get('prefix')
URL = CONFIG.get('request').get('url')

HEADERS = CONFIG.get('request').get('headers')
# ── Payload builder ───────────────────────────────────────────────────────────

def build_payload(order: str) -> str:
    if order == 'desc':
        payload = CONFIG.get('payloads').get('desc')
    elif order == 'asc':
        payload = CONFIG.get('payloads').get('asc')
    else:        raise ValueError(f"Invalid order: {order}")
    return payload

# ── Core helpers ──────────────────────────────────────────────────────────────
def fetch_ads(order: str) -> list[dict]:
    """Call the API and return the list of ad _source dicts."""
    payload = build_payload(order)
    resp = requests.post(URL, headers=HEADERS, data=payload, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    responses = data.get("responses", [])

    # get the ads from the right response based on the order
    ads_response = responses[-1]
    hits = ads_response.get("hits", {}).get("hits", [])
    hits = [hit["_source"] for hit in hits]
    elite_hits = responses[1].get('hits', {}).get('hits', [])
    elite_hits = [hit['_source'] for hit in elite_hits]
    featured_hits = responses[2].get('hits', {}).get('hits', [])
    featured_hits = [hit['_source'] for hit in featured_hits]
    hits.extend(elite_hits)
    hits.extend(featured_hits)
    return hits


def save_to_s3(cars: list[dict], label: str) -> str:
    """Save deduplicated cars to S3 and return the S3 key."""
    s3 = boto3.client("s3")
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{S3_PREFIX}/{date_str}/{label}.json"

    body = json.dumps({"count": len(cars), "cars": cars}, ensure_ascii=False)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return key


# ── Lambda handler ────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    print("Fetching newest cars …")
    newest = fetch_ads("desc")
    print(f"  → {len(newest)} ads from newest call")
    
    print("Fetching oldest cars …")
    oldest = fetch_ads("asc")
    print(f"  → {len(oldest)} ads from oldest call")
    
    print("Fetching featured cars …")
    featured = fetch_ads("featured")
    print(f"  → {len(featured)} ads from featured call")

    key = save_to_s3(oldest, "oldest_cars")
    print(f"Saved to s3://{S3_BUCKET}/{key}")
    key = save_to_s3(newest, "newest_cars")
    print(f"Saved to s3://{S3_BUCKET}/{key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "newest": len(newest),
            "oldest": len(oldest),
            "s3_key": key,
        }),
    }