FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2 python-dotenv==1.0.1
COPY app/producer.py /app/producer.py
RUN useradd -ms /bin/bash streamer && chown -R streamer:streamer /app
USER streamer

CMD ["python", "/app/producer.py"]

