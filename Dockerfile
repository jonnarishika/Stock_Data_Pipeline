FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install kafka-python requests

CMD ["python", "-u", "ingestion/kafka_producer.py"]