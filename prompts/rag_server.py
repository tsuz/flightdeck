"""
Lightweight RAG HTTP server that wraps Qdrant for the think-consumer.

Listens on port 8081 and exposes:
    POST /api/rag/query  { "query": "...", "top_k": 5, "session_id": "..." }

Returns:
    { "documents": [ { "text": "...", "score": 0.85, "category": "...", "tool": "..." }, ... ] }

Usage:
    python rag_server.py
"""

import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

COLLECTION_NAME = "prompts"
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
PORT = int(os.environ.get("PORT", "8081"))

# Initialize once at startup
print(f"Loading embedding model '{EMBEDDING_MODEL}'...")
model = SentenceTransformer(EMBEDDING_MODEL)
client = QdrantClient(url=QDRANT_URL)
print(f"RAG server ready — Qdrant at {QDRANT_URL}, collection '{COLLECTION_NAME}'")


class RagHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        if self.path != "/api/rag/query":
            self._send(404, {"error": "Not found"})
            return

        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length > 0 else {}

            query_text = body.get("query", "")
            top_k = body.get("top_k", 5)
            session_id = body.get("session_id", "unknown")

            if not query_text:
                self._send(400, {"error": "Missing 'query' field"})
                return

            # Embed and search
            embedding = model.encode(query_text).tolist()
            results = client.query_points(
                collection_name=COLLECTION_NAME,
                query=embedding,
                limit=top_k,
            )

            documents = [
                {
                    "text": point.payload.get("text", ""),
                    "score": point.score,
                    "category": point.payload.get("category", ""),
                    "tool": point.payload.get("tool", ""),
                }
                for point in results.points
            ]

            print(f"[{session_id}] query='{query_text[:60]}...' results={len(documents)}")
            self._send(200, {"documents": documents})

        except Exception as e:
            print(f"Error: {e}")
            self._send(500, {"error": str(e)})

    def _send(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        pass  # Suppress default access logs


def main():
    server = HTTPServer(("0.0.0.0", PORT), RagHandler)
    print(f"RAG server listening on http://0.0.0.0:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
        server.server_close()


if __name__ == "__main__":
    main()
