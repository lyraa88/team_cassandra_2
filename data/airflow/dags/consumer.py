import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
from datetime import datetime

# Cassandra 연결
cluster = Cluster(['cassandra'])
session = cluster.connect()

# 키스페이스 및 테이블 생성
session.execute("CREATE KEYSPACE IF NOT EXISTS log_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
session.set_keyspace('log_keyspace')
session.execute("""
    CREATE TABLE IF NOT EXISTS logs_by_ip (
        accessed_date timestamp,
        duration_secs int,
        network_protocol text,
        ip text,
        bytes int,
        accessed_from text,
        age int,
        gender text,
        country text,
        membership text,
        language text,
        sales float,
        returned text,
        returned_amount int,
        pay_method text,
        PRIMARY KEY (ip, accessed_date)
    ) WITH CLUSTERING ORDER BY (accessed_date DESC);
""")

print("Starting Kafka consumer...")
# Kafka 컨슈머 설정
consumer = KafkaConsumer(
    'log_data_topic',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 데이터 저장
for message in consumer:
    log_entry = message.value
    insert_stmt = session.prepare("""
        INSERT INTO logs_by_ip (
            accessed_date, duration_secs, network_protocol, ip, bytes, accessed_from,
            age, gender, country, membership, language, sales, returned,
            returned_amount, pay_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    
    # 타입 변환
    accessed_date = datetime.strptime(log_entry['accessed_date'], '%Y-%m-%d %H:%M:%S.%f')
    duration = int(log_entry['duration_secs'])
    sales = float(log_entry['sales'])
    
    try:
        session.execute(insert_stmt, (
            accessed_date, duration, log_entry['network_protocol'], log_entry['ip'],
            log_entry['bytes'], log_entry['accessed_from'], log_entry['age'],
            log_entry['gender'], log_entry['country'], log_entry['membership'],
            log_entry['language'], sales, log_entry['returned'],
            log_entry['returned_amount'], log_entry['pay_method']
        ))
        print(f"Saved to Cassandra: {log_entry['ip']} log entry")
    except Exception as e:
        print(f"Error saving to Cassandra: {e}")