import requests
import csv
import time

IMDB_ID = "tt33014583"
OUT_CSV = f"{IMDB_ID}_all_reviews.csv"

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

# Step 1: prime cookies
session.get(
    f"https://www.imdb.com/title/{IMDB_ID}/reviews/",
    headers=BASE_HEADERS,
    timeout=30,
)

print("[✓] Session initialized")

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

    reviews = data["data"]["title"]["reviews"]["edges"]
    page_info = data["data"]["title"]["reviews"]["pageInfo"]

    print(f"[+] Page {page}: {len(reviews)} reviews")

    for r in reviews:
        node = r["node"]
        all_reviews.append({
    "review_id": node.get("id"),
    "author": node.get("author", {})
                  .get("username", {})
                  .get("text", ""),
    "rating": node.get("rating"),
    "title": node.get("summary", {})
                .get("originalText", ""),
    "text": node.get("text", {})
               .get("originalText", {})
               .get("plaidHtml", ""),
    "helpful": node.get("helpfulness", {})
                  .get("upVotes"),
    "date": node.get("createdDate")  # may be None
})


    if not page_info["hasNextPage"]:
        print("[✓] No more pages")
        break

    cursor = page_info["endCursor"]
    page += 1
    time.sleep(0.8)  # polite delay

# Write CSV
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "review_id",
            "author",
            "rating",
            "title",
            "text",
            "helpful",
            "date"
        ]
    )
    writer.writeheader()
    writer.writerows(all_reviews)

print(f"[✓] Saved {len(all_reviews)} reviews to {OUT_CSV}")
