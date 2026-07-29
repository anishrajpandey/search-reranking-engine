import json
import time
import random
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "search-click-logs"