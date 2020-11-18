import argparse, sys
from confluent_kafka import avro, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4


name_schema = """
    {
        "namespace": "io.confluent.examples.clients.cloud",
        "name": "Name",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"}
        ]
    }
"""

class Name(object):
    __slots__ = ["name", "id"]

    def __init__(self, name=None):
        self.name = name
        self.id = uuid4()

    @staticmethod
    def dict_to_name(obj, ctx):
        return Name(obj['name'])

    @staticmethod
    def name_to_dict(name, ctx):
        return Name.to_dict(name)

    def to_dict(self):
        return dict(name=self.name)


count_schema = """
    {
        "namespace": "io.confluent.examples.clients.cloud",
        "name": "Count",
        "type": "record",
        "fields": [
            {"name": "count", "type": "int"}
        ]
    }
"""


class Count(object):
    __slots__ = ["count", "id"]

    def __init__(self, count=None):
        self.count = count
        self.id = uuid4()

    @staticmethod
    def dict_to_count(obj, ctx):
        return Count(obj['count'])

    @staticmethod
    def count_to_dict(count, ctx):
        return Count.to_dict(count)

    def to_dict(self):
        return dict(count=self.count)


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
             description="Confluent Python Client example to produce messages \
                  to Confluent Cloud")
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    required.add_argument('-f',
                          dest="config_file",
                          help="path to Confluent Cloud configuration file",
                          required=True)
    required.add_argument('-t',
                          dest="topic",
                          help="topic name",
                          required=True)
    args = parser.parse_args()

    return args


def read_ccloud_config(config_file):

    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()

    return conf


def create_topic(conf, topic):

    a = AdminClient({
           'bootstrap.servers': conf['bootstrap.servers'],
           'sasl.mechanisms': 'PLAIN',
           'security.protocol': 'SASL_SSL',
           'sasl.username': conf['sasl.username'],
           'sasl.password': conf['sasl.password']
    })
    fs = a.create_topics([NewTopic(
         topic,
         num_partitions=1,
         replication_factor=3
    )])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:

            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print("Failed to create topic {}: {}".format(topic, e))
                sys.exit(1)