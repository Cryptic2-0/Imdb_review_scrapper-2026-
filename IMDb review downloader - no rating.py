import requests
import csv
import time
import re

# -----------------------------
# Ask user for IMDb reviews URL
# -----------------------------
reviews_url = input("Enter IMDb reviews URL: ").strip()
match = re.search(r"(tt\d+)", reviews_url)
if not match:
    raise ValueError("Invalid IMDb URL: could not find ttXXXXXXX ID")

IMDB_ID = match.group(1)
OUT_CSV = f"{IMDB_ID}_all_reviews_no_rating.csv"

# -----------------------------
# Headers
# -----------------------------
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

# -----------------------------
# Step 1: Prime cookies
# -----------------------------
session.get(
    f"https://www.imdb.com/title/{IMDB_ID}/reviews/",
    headers=BASE_HEADERS,
    timeout=30,
)

print(f"[✓] Session initialized for {IMDB_ID}")

# -----------------------------
# CSV setup (fixed schema)
# -----------------------------
fieldnames = [
    "review_id",
    "author",
    "title",
    "text",
    "helpful",
    "date",
]

# -----------------------------
# Pagination loop
# -----------------------------
cursor = None
page = 1
rows_written = 0

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

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

        reviews = data["data"]["title"]["reviews"]["edges"]
        page_info = data["data"]["title"]["reviews"]["pageInfo"]

        print(f"[+] Page {page}: {len(reviews)} reviews")

        for r in reviews:
            node = r["node"]

            writer.writerow({
                "review_id": node.get("id", ""),
                "author": node.get("author", {})
                              .get("username", {})
                              .get("text", ""),
                "title": node.get("summary", {})
                            .get("originalText", "") if node.get("summary") else "",
                "text": node.get("text", {})
                           .get("originalText", {})
                           .get("plaidHtml", ""),
                "helpful": node.get("helpfulness", {})
                              .get("upVotes", ""),
                "date": node.get("createdDate", ""),
            })
            rows_written += 1

        if not page_info["hasNextPage"]:
            print("[✓] No more pages")
            break

        cursor = page_info["endCursor"]
        page += 1
        time.sleep(0.8)

print(f"[✓] Saved {rows_written} reviews to {OUT_CSV}")
