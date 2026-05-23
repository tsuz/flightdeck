FROM python:3.11-slim
WORKDIR /app

# Install SDK
COPY sdk/python /tmp/sdk
RUN pip install --no-cache-dir /tmp/sdk && rm -rf /tmp/sdk

COPY examples/lead-followup-agent-confluent-cloud/lead-tool-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/lead-followup-agent-confluent-cloud/lead-tool-service/app.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
