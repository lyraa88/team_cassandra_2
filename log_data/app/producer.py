import os, time, csv, sys
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logs_raw")
CSV_PATH = os.getenv("CSV_PATH", "/app/ingest/logs.csv")
RATE = float(os.getenv("SEND_RATE_PER_SEC", "50"))

def main():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    sent = 0
    delay = 1.0 / RATE if RATE > 0 else 0.0

    if not os.path.isfile(CSV_PATH):
        print(f"[ERROR] CSV not found: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    while True:  # keep looping forever
        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # overwrite accessed_date with now()
                row["accessed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                # key = item_category (must be bytes)
                key = row["item_category"].encode("utf-8")

                # payload as string
                payload = str(row).encode("utf-8")

                producer.send(TOPIC, key=key, value=payload)
                sent += 1

                if delay > 0:
                    time.sleep(delay)
                if sent % 100 == 0:
                    print(f"sent={sent}", flush=True)

        # after finishing one pass, loop again (with fresh timestamps)

    producer.flush()
    print(f"Done. total sent={sent}")

if __name__ == "__main__":
    main()
