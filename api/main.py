import json
import time
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis
from confluent_kafka import Producer

app = FastAPI(title="Real-Time Search Re-Ranking Engine", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. Connect to Redis & Kafka
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
producer = Producer({"bootstrap.servers": "localhost:9092"})
TOPIC_NAME = "search-click-logs"

# 2. Baseline Document Catalog (Default static rank before real-time boosts)
DOCUMENT_CATALOG = {
    "wireless headphones": [
        {"id": "doc_apple_airpods", "title": "Apple AirPods Pro (2nd Gen)", "base_rank": 1},
        {"id": "doc_sony_wh1000", "title": "Sony WH-1000XM5 Noise Canceling", "base_rank": 2},
        {"id": "doc_bose_700", "title": "Bose Noise Cancelling 700", "base_rank": 3},
    ],
    "mechanical keyboard": [
        {"id": "doc_keychron_k2", "title": "Keychron K2 Wireless Mechanical", "base_rank": 1},
        {"id": "doc_logitech_mx", "title": "Logitech MX Mechanical Mini", "base_rank": 2},
        {"id": "doc_gmmk_pro", "title": "Glorious GMMK Pro Custom Keyboard", "base_rank": 3},
    ],
    "4k monitor": [
        {"id": "doc_dell_u2720q", "title": "Dell UltraSharp U2720Q 27-inch 4K", "base_rank": 1},
        {"id": "doc_lg_27uk850", "title": "LG 27UK850-W 27-Inch 4K UHD", "base_rank": 2},
        {"id": "doc_asus_proart", "title": "ASUS ProArt Display PA279CV 4K", "base_rank": 3},
    ],
    "gaming laptop": [
        {"id": "doc_rog_zephyrus", "title": "ASUS ROG Zephyrus G14 Gaming Laptop", "base_rank": 1},
        {"id": "doc_lenovo_legion", "title": "Lenovo Legion Pro 7i Gen 8 Intel", "base_rank": 2},
        {"id": "doc_razer_blade", "title": "Razer Blade 16 Gaming Laptop 240Hz", "base_rank": 3},
    ],
    "smartwatch": [
        {"id": "doc_apple_watch", "title": "Apple Watch Series 9 GPS 45mm", "base_rank": 1},
        {"id": "doc_galaxy_watch", "title": "Samsung Galaxy Watch 6 Classic", "base_rank": 2},
        {"id": "doc_garmin_epix", "title": "Garmin Epix Pro Gen 2 Sapphire", "base_rank": 3},
    ],
    "noise canceling earbuds": [
        {"id": "doc_sony_wf1000", "title": "Sony WF-1000XM5 Wireless Earbuds", "base_rank": 1},
        {"id": "doc_bose_qc", "title": "Bose QuietComfort Ultra Earbuds", "base_rank": 2},
        {"id": "doc_sennheiser_tw4", "title": "Sennheiser Momentum True Wireless 4", "base_rank": 3},
    ]
}

class ClickEvent(BaseModel):
    query: str
    clicked_doc_id: str
    rank_position: int

@app.get("/search")
def search(q: str):
    """
    Fetches documents for a query and dynamically re-ranks them
    using real-time IPS scores stored in Redis.
    """
    normalized_query = q.lower().strip()
    if normalized_query not in DOCUMENT_CATALOG:
        raise HTTPException(status_code=404, detail="Query not found in catalog")

    docs = DOCUMENT_CATALOG[normalized_query]
    redis_key = f"rerank:{normalized_query}"

    # Fetch live scores from Redis Sorted Set (ZSET)
    live_scores = r.zrange(redis_key, 0, -1, withscores=True)
    score_map = {doc_id: score for doc_id, score in live_scores}

    # Re-rank: Higher Redis score comes first; falls back to initial base_rank
    reranked_docs = []
    for doc in docs:
        realtime_score = score_map.get(doc["id"], 0.0)
        reranked_docs.append({
            "id": doc["id"],
            "title": doc["title"],
            "base_rank": doc["base_rank"],
            "realtime_score": round(realtime_score, 2)
        })

    # Sort descending by real-time score
    reranked_docs.sort(key=lambda x: x["realtime_score"], reverse=True)

    return {
        "query": normalized_query,
        "results_count": len(reranked_docs),
        "results": reranked_docs
    }

@app.post("/click")
def record_click(event: ClickEvent):
    """
    Receives an actual user click event and streams it into Kafka.
    """
    payload = {
        "query": event.query.lower().strip(),
        "clicked_doc_id": event.clicked_doc_id,
        "rank_position": event.rank_position,
        "timestamp": int(time.time())
    }

    # Emit event to Kafka
    producer.produce(
        topic=TOPIC_NAME,
        key=payload["query"],
        value=json.dumps(payload).encode("utf-8")
    )
    producer.poll(0)

    return {"status": "success", "message": "Click event emitted to Kafka", "data": payload}