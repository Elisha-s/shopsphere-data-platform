from ingestion.consumers.kafka_consumer import consume_events
import sys

if __name__ == "__main__":

    if len(sys.argv) > 1:
        consumer_name = sys.argv[1]
    else:
        consumer_name = "Consumer"
    consume_events(consumer_name)