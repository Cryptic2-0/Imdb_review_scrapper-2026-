import requests
import csv
import time
import os
import json
from datetime import datetime

IMDB_ID = "tt33014583"

# ---------------- SAFETY: versioned output ----------------
BASE_NAME = f"{IMDB_ID}_all_reviews"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV = f"{BASE_NAME}_{TIMESTAMP}.csv"

RAW_DIR = f"{IMDB_ID}_raw_pages"
os.makedirs(RAW_DIR, exist_ok=True)

# ---------------- HEADERS ----------------
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}

GRAPHQL_HEADERS = {
    "User-Agent": BASE_HEADERS["User-Agent"],
    "Accept": "application/graphql+json, application/json",
    "Content-Type": "application/json",
    "Origin": "https://www.imdb.com",
    "Referer": f"https://www.imdb.com/title/{IMDB_ID}/reviews/",
    "x-imdb-client-name": "imdb-web-next",
    "x-imdb-client-version": "1.0.0",
}

session = requests.Session()

# ---------------- PRIME SESSION ----------------
session.get(
    f"https://www.imdb.com/title/{IMDB_ID}/reviews/",
    headers=BASE_HEADERS,
    timeout=30,
)

print("[✓] Session initialized")

# ---------------- LOAD EXISTING REVIEW IDS (IF ANY) ----------------
existing_ids = set()

for file in os.listdir("."):
    if file.startswith(BASE_NAME) and file.endswith(".csv"):
        with open(file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("review_id"):
                    existing_ids.add(row["review_id"])

if existing_ids:
    print(f"[i] Found {len(existing_ids)} existing reviews (will skip duplicates)")

# ---------------- FETCH LOOP ----------------
all_reviews = []
cursor = None
page = 1

while True:
    payload = {
        "operationName": "TitleReviewsRefine",
        "variables": {
            "after": cursor,
            "const": IMDB_ID,
            "filter": {},
            "first": 25,
            "locale": "en-US",
            "sort": {
                "by": "HELPFULNESS_SCORE",
                "order": "DESC"
            }
        },
        "extensions": {
            "persistedQuery": {
                "sha256Hash": "d389bc70c27f09c00b663705f0112254e8a7c75cde1cfd30e63a2d98c1080c87",
                "version": 1
            }
        }
    }

    resp = session.post(
        "https://caching.graphql.imdb.com/",
        headers=GRAPHQL_HEADERS,
        json=payload,
        timeout=30
    )

    resp.raise_for_status()
    data = resp.json()

    # --------- SAVE RAW PAGE (AUDIT SAFE) ---------
    with open(f"{RAW_DIR}/page_{page:03d}.json", "w", encoding="utf-8") as jf:
        json.dump(data, jf, ensure_ascii=False, indent=2)

    reviews = data["data"]["title"]["reviews"]["edges"]
    page_info = data["data"]["title"]["reviews"]["pageInfo"]

    print(f"[+] Page {page}: {len(reviews)} reviews")

    for r in reviews:
        node = r["node"]
        rid = node.get("id")

        # --------- SKIP DUPLICATES ---------
        if rid in existing_ids:
            continue

        all_reviews.append({
            "review_id": rid,
            "author": node.get("author", {})
                          .get("username", {})
                          .get("text", ""),
            "rating": node.get("rating"),
            "title": node.get("summary", {})
                        .get("originalText", ""),
            "text": node.get("text", {})
                       .get("originalText", {})
                       .get("plaidHtml", ""),
            "helpful_upvotes": node.get("helpfulness", {})
                                  .get("upVotes"),
            "helpful_downvotes": node.get("helpfulness", {})
                                    .get("downVotes"),
            "date": node.get("createdDate")
        })

        existing_ids.add(rid)

    if not page_info["hasNextPage"]:
        print("[✓] No more pages")
        break

    cursor = page_info["endCursor"]
    page += 1
    time.sleep(0.8)

# ---------------- WRITE CSV (NON-DESTRUCTIVE) ----------------
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "review_id",
            "author",
            "rating",
            "title",
            "text",
            "helpful_upvotes",
            "helpful_downvotes",
            "date"
        ]
    )
    writer.writeheader()
    writer.writerows(all_reviews)

print(f"[✓] Saved {len(all_reviews)} NEW reviews → {OUT_CSV}")
print(f"[✓] Raw pages archived in → {RAW_DIR}/")
