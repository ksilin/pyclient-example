import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import argparse
from confluent_kafka import SerializingProducer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.admin import AdminClient
from common.client_factory import create_producer_from_files, load_config
from producer.model.person import Person
from producer.model.personAddedJob import PersonAddedJob
from producer.model.person_serde_util import person_to_dict, generate_random_person
from common.utils import create_kafka_topics, acked
import logging

kafka_logger = logging.getLogger('confluent_kafka')
kafka_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
#handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
kafka_logger.addHandler(handler)

def produce_message(producer: SerializingProducer, person: Person, topic: str):
    try:
        producer.produce(topic=topic, key=person.lastName, value=person_to_dict(person), on_delivery=acked)
        producer.flush()
    except Exception as e:
        kafka_logger.error(f"Failed to produce message: {e}")

def produce_message2(producer: Producer, value_serializer, person: Person, topic: str):
    try:
        value = value_serializer(
            person_to_dict(person),
            SerializationContext(topic, MessageField.VALUE)
        )
        producer.produce(
            topic=topic,
            key=person.lastName,
            value=value
            ,on_delivery=acked
        )
        producer.flush()
    except Exception as e:
        kafka_logger.error(f"Failed to produce message: {e}")

def main():
    parser = argparse.ArgumentParser(description="Kafka Producer for Person")
    parser.add_argument('--config', type=str, required=True, help="Path to the producer config file")
    parser.add_argument('--schema', type=str, required=True, help="Path to the schema file")
    parser.add_argument('--limit', type=int, default=-1, help="Number of messages to produce. Default is unlimited (-1)")
    parser.add_argument('--interval', type=int, default=5, help="Time interval between messages in seconds. Default is 5 seconds")

    args = parser.parse_args()

    config_file = args.config
    schema_file = args.schema
    limit = args.limit
    interval = args.interval

    TOPIC = "person_topic"

    config = load_config(config_file)
    KAFKA_BROKER = config['bootstrap.servers']
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    create_kafka_topics(admin_client, [TOPIC])

    serializing = False

    producer, value_serializer = create_producer_from_files(config_file, schema_file, serializing=serializing)

    message_count = 0
    while limit == -1 or message_count < limit:
        person = person = generate_random_person()
        if(serializing): produce_message(producer, person, TOPIC)
        else: produce_message2(producer, value_serializer, person, TOPIC)
        
        message_count += 1
        kafka_logger.info(f"Produced {message_count} messages")
        time.sleep(interval)

if __name__ == "__main__":
    main()
