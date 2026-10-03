"""m2_create_topics.py - creates the three Kafka topics (safe to run more than once)."""
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import config

try:
    admin = KafkaAdminClient(bootstrap_servers=[config.KAFKA_BROKER], request_timeout_ms=10000)
except Exception as e:
    sys.exit(f"Could not connect to Kafka at {config.KAFKA_BROKER}: {e}\n"
             "Is Docker Desktop running, and did you run: docker compose up -d ?")

topics = [
    NewTopic(config.TOPIC_EVENTS, num_partitions=config.TOPIC_PARTITIONS, replication_factor=1),
    NewTopic(config.TOPIC_RECS, num_partitions=config.TOPIC_PARTITIONS, replication_factor=1),
    NewTopic(config.TOPIC_ALERTS, num_partitions=1, replication_factor=1),
]
for t in topics:
    try:
        admin.create_topics([t])
        print(f"  created topic '{t.name}' ({t.num_partitions} partitions)")
    except TopicAlreadyExistsError:
        print(f"  topic '{t.name}' already exists")
print("Topics ready:", sorted(admin.list_topics()))
