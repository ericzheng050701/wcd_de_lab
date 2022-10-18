# Import necessary libraries
import json
import argparse
from sseclient import SSEClient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from typing import Type

# function to create Kafka Producer
# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html


def create_kafka_producer(bootstrap_server: str, acks: str) -> Type[KafkaProducer]:
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            acks=acks,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)


def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')

    parser.add_argument('--bootstrap_server', default='localhost:9092',
                        help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--topic_name', default='wikipedia-events',
                        help='Destination topic name', type=str)
    parser.add_argument('--acks', default='all',
                        help='Kafka Producer acknowledgment', type=str)
    parser.add_argument('--events_to_produce',
                        help='Kill producer after n events have been produced', type=int, default=1000)

    return parser.parse_args()


if __name__ == "__main__":

    # parse command line arguments
    args = parse_command_line_arguments()

    # init Kafka producer
    producer = create_kafka_producer(args.bootstrap_server, args.acks)

    # consume websocket
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'

    print(f'Messages are being published to Kafka topic {args.topic_name}')
    messages_count = 0

    for event in SSEClient(url):
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
                producer.send(args.topic_name, value=event_data)
                messages_count += 1
            except ValueError as err:
                pass

        if messages_count >= args.events_to_produce:
            print(f'Producer will be killed as {args.events_to_produce} events were producted')
            exit(0)
