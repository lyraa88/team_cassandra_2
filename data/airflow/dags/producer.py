import json
import time
from kafka import KafkaProducer

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 데이터 파일 경로
file_path = '/opt/airflow/dags/data/your_kaggle_data.csv'  # 실제 파일명으로 수정

print("Starting producer...")
with open(file_path, 'r', encoding='utf-8') as file:
    # CSV 헤더 건너뛰기
    header = next(file)
    for line in file:
        line = line.strip()
        if not line:
            continue
        
        # CSV 라인을 딕셔너리로 변환
        # 데이터 구조에 따라 이 부분을 수정해야 합니다.
        try:
            data_dict = {
                "accessed_date": line.split(',')[0],
                "duration_secs": int(line.split(',')[1].strip()),
                "network_protocol": line.split(',')[2].strip(),
                "ip": line.split(',')[3].strip(),
                "bytes": int(line.split(',')[4].strip()),
                # 나머지 필드도 동일하게 파싱
            }
            producer.send('log_data_topic', value=data_dict)
            print(f"Sent: {data_dict['ip']} log entry")
            time.sleep(0.1) # 전송 간격 조절
        except Exception as e:
            print(f"Error processing line: {line}. Error: {e}")