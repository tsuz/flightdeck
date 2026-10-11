FROM python:3.11-slim
WORKDIR /app

COPY sdk/python /tmp/sdk
RUN pip install --no-cache-dir /tmp/sdk && rm -rf /tmp/sdk

COPY examples/helpdesk-router/finance-agent/tool-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/helpdesk-router/finance-agent/tool-service/app.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
