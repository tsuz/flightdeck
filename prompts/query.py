"""
Queries the Qdrant collection for documents relevant to a user query.

Usage:
    python query.py "schedule a meeting with John tomorrow"
    python query.py "what's the weather in Tokyo"
"""

import sys
import json

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

COLLECTION_NAME = "prompts"
QDRANT_URL = "http://localhost:6333"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
TOP_K = 3


def query(text: str, top_k: int = TOP_K):
    client = QdrantClient(url=QDRANT_URL)
    model = SentenceTransformer(EMBEDDING_MODEL)

    embedding = model.encode(text).tolist()

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=embedding,
        limit=top_k,
    )

    return results.points


def main():
    if len(sys.argv) < 2:
        print("Usage: python query.py <query text>")
        sys.exit(1)

    text = " ".join(sys.argv[1:])
    print(f"Query: {text}\n")

    results = query(text)
    for i, point in enumerate(results):
        print(f"--- Result {i + 1} (score: {point.score:.4f}) ---")
        print(f"  Category: {point.payload.get('category', 'N/A')}")
        print(f"  Tool:     {point.payload.get('tool', 'N/A')}")
        print(f"  Text:     {point.payload['text'][:120]}...")
        print()


if __name__ == "__main__":
    main()
