import csv
import json
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

producer = Producer({"bootstrap.servers": "kafka:29092"})
TOPIC = "electronic-store"
OUT_TOPIC = "electronic-analytics"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            "Message delivered to {} [{}]".format(
                msg.topic(), msg.value().decode("utf-8")
            )
        )


def create_topics():
    admin_client = AdminClient({"bootstrap.servers": "kafka:29092"})
    topic_list = [NewTopic(x) for x in [TOPIC, OUT_TOPIC]]
    admin_client.create_topics(topic_list)


create_topics()

"""
Reads the dataset and sends each row as a JSON-encoded message to the Kafka topic.
"""
with open("dataset.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        producer.poll(0)
        msg = json.dumps(row)
        producer.produce(TOPIC, msg.encode("utf-8"), callback=delivery_report)
        time.sleep(0.1)
