FROM python:3.11-slim
WORKDIR /app

RUN pip install --no-cache-dir "qdrant-client>=1.12.0,<1.13.0" sentence-transformers>=3.0.0

COPY seed-data/seed.py .
COPY docs/ ./docs/

ENV PYTHONUNBUFFERED=1
ENV QDRANT_URL=http://qdrant:6333
ENV DOCS_DIR=/app/docs
CMD ["python", "seed.py"]
