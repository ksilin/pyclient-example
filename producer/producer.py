import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient
from common.client_factory import create_producer_from_files, load_config
from producer.model.person import Person
from producer.model.personAddedJob import PersonAddedJob
from common.utils import create_kafka_topics, acked

config_file = os.path.join(os.path.dirname(__file__), "producer_config.json")
schema_file = os.path.join(os.path.dirname(__file__), "../schema/person_v1.json")

config = load_config(config_file)

KAFKA_BROKER = config['bootstrap.servers']
TOPIC = "person_topic"


admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
create_kafka_topics(admin_client, [TOPIC]) 

def produce_message(producer: SerializingProducer, person: Person):
    """Produce a message with the given producer"""
    try:
        producer.produce(topic=TOPIC, key="person_key", value=person_to_dict(person), on_delivery=acked)
        producer.flush()
    except Exception as e:
        print(f"Failed to produce message: {e}")

def person_to_dict(person: Person) -> dict:
    return {
        "firstName": person.firstName,
        "lastName": person.lastName,
        "age": person.age
    }

def person_job_added_to_dict(person: PersonAddedJob) -> dict:
    return {
        "firstName": person.firstName,
        "lastName": person.lastName,
        "job": person.job,
        "age": person.age
    }

def person_from_dict(obj, ctx):
    return Person(obj["firstName"], obj["lastName"], obj.get("age",0))


def main():
    producer = create_producer_from_files(config_file, schema_file)

    interval = 5
    while True:
        person = Person("John", "Doe", 30)
        produce_message(producer, person)
        time.sleep(interval)

if __name__ == "__main__":
    main()
