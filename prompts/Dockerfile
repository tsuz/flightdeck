FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV QDRANT_URL=http://qdrant:6333
ENV PORT=8081

EXPOSE 8081
CMD ["python", "rag_server.py"]
