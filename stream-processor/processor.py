import json
import redis
from confluent_kafka import Consumer, KafkaError

# 1. Connect to Redis running in Docker
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 2. Configure Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'reranker-consumer-group-v2',  # Fresh group ID resets historical offset tracking
    'auto.offset.reset': 'earliest'            # Read from the start of available messages
}
consumer = Consumer(conf)
TOPIC_NAME = 'search-click-logs'
consumer.subscribe([TOPIC_NAME])

def calculate_ips_weight(rank_position: int) -> float:
    """
    Inverse Propensity Scoring (IPS):
    Lower-ranked items that get clicked receive a higher weight boost
    to counteract position bias.
    """
    return round(float(rank_position) ** 0.5, 3)

def start_processing():
    print("🧠 Stream Processor listening for Kafka events... (Press Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka Error: {msg.error()}")
                    break

            # Safely parse event payload
            try:
                if msg.value() is None:
                    continue
                event_data = json.loads(msg.value().decode('utf-8'))
                query = event_data['query']
                clicked_doc = event_data['clicked_doc_id']
                rank_position = event_data['rank_position']

                # Calculate bias-corrected score boost
                score_boost = calculate_ips_weight(rank_position)

                # Update Redis Sorted Set (ZSET)
                # Key format: "rerank:<query>"
                redis_key = f"rerank:{query}"
                r.zincrby(redis_key, score_boost, clicked_doc)

                # Fetch new top score for logging
                current_score = r.zscore(redis_key, clicked_doc)
                print(f"⚡ [Event Processed] Query: '{query}' | Doc: '{clicked_doc}' (Rank #{rank_position}) -> +{score_boost} pts | New Total: {current_score:.2f}", flush=True)
            except Exception as e:
                print(f"⚠️ [Skipped Event] Could not process Kafka message: {e}", flush=True)
                continue

    except KeyboardInterrupt:
        print("\n🛑 Shutting down stream processor...", flush=True)
    finally:
        consumer.close()

if __name__ == "__main__":
    start_processing()