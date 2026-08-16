import json
import time
import random
from confluent_kafka import Producer

# Connect to Kafka running inside Docker on port 9092
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "search-click-logs"

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# Mock dataset simulating user searches
MOCK_QUERIES = ["wireless headphones", "mechanical keyboard", "4k monitor"]
MOCK_DOCUMENTS = {
    "wireless headphones": ["doc_apple_airpods", "doc_sony_wh1000", "doc_bose_700"],
    "mechanical keyboard": ["doc_keychron_k2", "doc_logitech_mx", "doc_gmmk_pro"],
    "4k monitor": ["doc_dell_u2720q", "doc_lg_27uk850", "doc_asus_proart"]
}

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Click event delivered to {msg.topic()} [{msg.partition()}]")

def start_producing():
    print("Kafka Producer active. Simulating live user click stream... (Press Ctrl+C to stop)")
    try:
        while True:
            query = random.choice(MOCK_QUERIES)
            available_docs = MOCK_DOCUMENTS[query]
            clicked_doc = random.choice(available_docs)
            rank_position = available_docs.index(clicked_doc) + 1

            payload = {
                "query": query,
                "clicked_doc_id": clicked_doc,
                "rank_position": rank_position,
                "timestamp": int(time.time())
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=query,
                value=json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(random.uniform(0.3, 1.2)) 
            
    except KeyboardInterrupt:
        print("\n Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    start_producing()