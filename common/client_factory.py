import json
import copy
from confluent_kafka import DeserializingConsumer, Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import (
    Deserializer,
    Serializer,
    StringDeserializer,
    StringSerializer,
)
from typing import Tuple


def load_schema(schema_file: str) -> str | None:
    schema_str = None
    if schema_file:
        with open(schema_file) as file:
            schema_str = file.read()

    return schema_str

def load_config(config_file: str) -> dict[str, str]:
    with open(config_file) as file:

        return json.load(file)

def split_config(conf: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    schema_registry_conf = {}
    producer_conf = {}
    for k, v in conf.items():
        if 'schema.registry' in k:
            schema_registry_conf[k.replace('schema.registry.', '')] = v
        else:
            producer_conf[k] = v

    return schema_registry_conf, producer_conf

def create_serializer(schema_registry_conf: dict[str, str], schema_str: str | None, serializer_class, to_dict=None, serializer_conf=None) -> Serializer:
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    return serializer_class(schema_str=schema_str, schema_registry_client=schema_registry_client, to_dict=to_dict, conf=serializer_conf)

def create_producer_from_files(
    config_file: str, 
    schema_file: str, 
    serializer_class=JSONSerializer, 
    serializing=True,
    to_dict=None, 
    serializer_conf=None
) -> Tuple[Producer, Serializer]:

    conf = load_config(config_file)
    schema_str = load_schema(schema_file)
    schema_registry_conf, producer_conf = split_config(conf)

    value_serializer = create_serializer(schema_registry_conf, schema_str, serializer_class, to_dict, serializer_conf)

    if serializing:
        return create_serializing_producer(producer_conf, value_serializer), value_serializer
    else:
        return create_producer(producer_conf), value_serializer

def create_serializing_producer(producer_conf: dict[str, str], value_serializer: Serializer) -> SerializingProducer:
    producer_config = copy.deepcopy(producer_conf)
    producer_config['key.serializer'] = StringSerializer('utf_8')
    producer_config['value.serializer'] = value_serializer
    producer_config['debug']='all'
    producer_config['log_level'] = 7

    return SerializingProducer(producer_config)

def create_producer(producer_conf: dict[str, str]) -> Producer:
    producer_config = copy.deepcopy(producer_conf)
    producer_config['debug']='all'
    producer_config['log_level'] = 7
    return Producer(producer_config)

def create_deserializer(schema_registry_conf: dict[str, str], schema_str: str | None) -> JSONDeserializer:
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    return JSONDeserializer(schema_str=schema_str, schema_registry_client=schema_registry_client)

def create_consumer_from_files(config_file: str, schema_file: str, group_id: str) -> DeserializingConsumer:
    conf = load_config(config_file)
    schema_str = load_schema(schema_file)
    schema_registry_conf, consumer_conf = split_config(conf)
    value_deserializer = create_deserializer(schema_registry_conf, schema_str)

    return create_consumer(consumer_conf, value_deserializer, group_id)

def create_consumer(consumer_conf: dict[str, str], value_deserializer: Deserializer, group_id: str) -> DeserializingConsumer:
    consumer_conf['key.deserializer'] = StringDeserializer('utf_8')
    consumer_conf['value.deserializer'] = value_deserializer
    consumer_conf['group.id'] = group_id
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = DeserializingConsumer(consumer_conf)

    return consumer
