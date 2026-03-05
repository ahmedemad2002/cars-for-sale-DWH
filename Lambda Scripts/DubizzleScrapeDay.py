import json
import boto3
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET = "dubizzle-bronze"
S3_PREFIX = "raw/merged"
URL = (
    "https://search.olx.com.eg/_msearch"
    "?filter_path=took%2C*.took%2C*.timed_out%2C*.suggest.*.options.text%2C"
    "*.suggest.*.options._source.*%2C*.hits.total.*%2C*.hits.hits._source.*%2C"
    "*.hits.hits._score%2C*.hits.hits.highlight.*%2C*.error%2C"
    "*.aggregations.*.buckets.key%2C*.aggregations.*.buckets.doc_count%2C"
    "*.aggregations.*.buckets.complex_value.hits.hits._source%2C"
    "*.aggregations.*.filtered_agg.facet.buckets.key%2C"
    "*.aggregations.*.filtered_agg.facet.buckets.doc_count%2C"
    "*.aggregations.*.filtered_agg.facet.buckets.complex_value.hits.hits._source"
)
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.5",
    "authorization": "Basic b2x4LWVnLXByb2R1Y3Rpb24tc2VhcmNoOn1nNDM2Q0R5QDJmWXs2alpHVGhGX0dEZjxJVSZKbnhL",
    "content-type": "application/x-ndjson",
    "origin": "https://www.dubizzle.com.eg",
}

# ── Payload builder ───────────────────────────────────────────────────────────
# The first 3 sub-requests (aggregations + elite/featured) stay the same.
# Only the last sub-request changes between the two calls (sort order).
AGG_LINES = """\
{"index":"olx-eg-production-ads-ar"}
{"from":0,"size":0,"track_total_hits":false,"query":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}}]}},"aggs":{"category.lvl1.externalID":{"global":{},"aggs":{"filtered_agg":{"filter":{"bool":{"must":[{"term":{"category.lvl0.externalID":"129"}},{"term":{"location.externalID":"0-1"}}]}},"aggs":{"facet":{"terms":{"field":"category.lvl1.externalID","size":20}}}}}},"category.lvl2.externalID":{"global":{},"aggs":{"filtered_agg":{"filter":{"bool":{"must":[{"term":{"category.lvl1.externalID":"23"}},{"term":{"location.externalID":"0-1"}}]}},"aggs":{"facet":{"terms":{"field":"category.lvl2.externalID","size":20}}}}}},"location.lvl1":{"global":{},"aggs":{"filtered_agg":{"filter":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}},{"term":{"location.lvl0.externalID":"0-1"}}]}},"aggs":{"facet":{"terms":{"field":"location.lvl1.externalID","size":20},"aggs":{"complex_value":{"top_hits":{"size":1,"_source":{"include":["location.lvl1"]}}}}}}}}},"extraFields.make":{"global":{},"aggs":{"filtered_agg":{"filter":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}},{"term":{"location.externalID":"0-1"}}]}},"aggs":{"facet":{"terms":{"field":"extraFields.make","size":80}}}}}},"extraFields.model":{"global":{},"aggs":{"filtered_agg":{"filter":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}},{"term":{"location.externalID":"0-1"}}]}},"aggs":{"facet":{"terms":{"field":"extraFields.model","size":40}}}}}},"type":{"global":{},"aggs":{"filtered_agg":{"filter":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}},{"term":{"location.externalID":"0-1"}}]}},"aggs":{"facet":{"terms":{"field":"type","size":20}}}}}}},"timeout":"5s"}
{"index":"olx-eg-production-ads-ar"}
{"from":10,"size":0,"track_total_hits":200000,"query":{"function_score":{"random_score":{"seed":637},"query":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}},{"term":{"product":"elite"}},{"term":{"location.externalID":"0-1"}}]}}}},"sort":["_score"],"timeout":"5s"}
{"index":"olx-eg-production-ads-ar"}
{"from":40,"size":0,"track_total_hits":200000,"query":{"function_score":{"random_score":{"seed":637},"query":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}},{"term":{"product":"featured"}},{"term":{"location.externalID":"0-1"}}]}}}},"sort":["_score"],"timeout":"5s"}
"""

def _ads_line(order: str) -> str:
    """Return the ndjson line that fetches 10k ads sorted by timestamp."""
    return (
        '{"index":"olx-eg-production-ads-ar"}\n'
        '{"from":0,"size":10000,"track_total_hits":200000,'
        '"query":{"bool":{"must":[{"term":{"category.slug":"cars-for-sale"}},'
        '{"term":{"location.externalID":"0-1"}}],'
        '"must_not":[{"term":{"product":"elite"}}]}},'
        f'"sort":[{{"timestamp":{{"order":"{order}"}}}},{{"id":{{"order":"{order}"}}}}],'
        '"timeout":"5s"}\n'
    )

def build_payload(order: str) -> str:
    return AGG_LINES + _ads_line(order)


# ── Core helpers ──────────────────────────────────────────────────────────────
def fetch_ads(order: str) -> list[dict]:
    """Call the API and return the list of ad _source dicts."""
    payload = build_payload(order)
    resp = requests.post(URL, headers=HEADERS, data=payload, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    responses = data.get("responses", [])

    # The ads response is the last one (index -1); hits are under hits.hits
    ads_response = responses[-1]
    hits = ads_response.get("hits", {}).get("hits", [])
    return [hit["_source"] for hit in hits]


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

    # Deduplicate by ad id — keep insertion order (newest first)
    seen: set[str] = set()
    merged: list[dict] = []
    newest_cars = newest['responses'][3]['hits']['hits']
    oldest_cars = oldest['responses'][3]['hits']['hits']
    for car in newest_cars + oldest_cars:
        ad_id = car.get('_source', {}).get('id')
        if ad_id not in seen:
            seen.add(ad_id)
            merged.append(car)

    print(f"Merged unique cars: {len(merged)}")

    key = save_to_s3(merged, "cars_all")
    print(f"Saved to s3://{S3_BUCKET}/{key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "newest": len(newest),
            "oldest": len(oldest),
            "unique": len(merged),
            "s3_key": key,
        }),
    }