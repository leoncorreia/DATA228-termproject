import csv
from json import dumps
from kafka import KafkaProducer

class CSVProducer:
    def __init__(self, csv_files: dict):
        self.csv_files = csv_files
        self.producers = {}
        for topic, csv_file in csv_files.items():
            self.producers[topic] = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
                acks='all'
            )

    def start_streams(self):
        for topic, csv_file in self.csv_files.items():
            with open(csv_file, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        self.producers[topic].send(topic, value=row)
                        self.producers[topic].flush()
                        print(f"CSV row sent to Kafka topic '{topic}': {row}")
                    except Exception as e:
                        print(f"An error occurred while sending to Kafka topic '{topic}': {str(e)}")

if __name__ == "__main__":
    csv_files = {
        "leie": "./leie.csv",
        "payment": "./payment.csv"
        # "meds": "./meds.csv"
    }
    csv_producer = CSVProducer(csv_files)
    csv_producer.start_streams()
