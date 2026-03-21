FROM python:3.11-slim
WORKDIR /app

# Install SDK
COPY sdk/python /tmp/sdk
RUN pip install --no-cache-dir /tmp/sdk && rm -rf /tmp/sdk

COPY examples/system-ops/kafka-tool-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/system-ops/kafka-tool-service/app.py .

ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV TARGET_KAFKA_BOOTSTRAP_SERVERS=kafka:29092

CMD ["python", "app.py"]
