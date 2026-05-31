FROM python:3.11-slim
WORKDIR /app

# Install the FlightDeck Python SDK (used here only for kafka_env_props).
COPY sdk/python /tmp/sdk
RUN pip install --no-cache-dir /tmp/sdk && rm -rf /tmp/sdk

COPY examples/multi-agent-setup/orchestrator/dispatcher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/multi-agent-setup/orchestrator/dispatcher/app.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
