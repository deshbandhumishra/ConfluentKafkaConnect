# ===Helper module ======================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib

if __name__ == '__main__':

    # config_file = "-f F:\pythonProject\pythonProject1\resources\ccloud_config.config"
    #topic = " -t pythontest1"


    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    print("========="+config_file)
    print("\n=========" + topic)
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })
    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0


    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))


    for n in range(10):
        record_key = "Taneja"
        record_value = json.dumps({'count': n})
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)

        producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
