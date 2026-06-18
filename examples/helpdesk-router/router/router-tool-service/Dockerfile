FROM python:3.11-slim
WORKDIR /app

# Install SDK
COPY sdk/python /tmp/sdk
RUN pip install --no-cache-dir /tmp/sdk && rm -rf /tmp/sdk

COPY examples/helpdesk-router/router/router-tool-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/helpdesk-router/router/router-tool-service/app.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
