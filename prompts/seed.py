"""
Seeds the Qdrant collection from tools.json — the single source of truth
for both tool schemas (Claude API) and prompt context (RAG).

Usage:
    pip install -r requirements.txt
    docker compose up -d
    python seed.py
"""

import json
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer

COLLECTION_NAME = "prompts"
QDRANT_URL = "http://localhost:6333"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # 384-dim, fast and lightweight
VECTOR_SIZE = 384


def main():
    client = QdrantClient(url=QDRANT_URL)
    model = SentenceTransformer(EMBEDDING_MODEL)

    # Create or recreate collection
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME in collections:
        print(f"Deleting existing collection '{COLLECTION_NAME}'")
        client.delete_collection(COLLECTION_NAME)

    print(f"Creating collection '{COLLECTION_NAME}' (dim={VECTOR_SIZE})")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
    )

    # Load tools.json — single source of truth
    tools_path = Path(__file__).parent / "tools.json"
    with open(tools_path) as f:
        tools = json.load(f)

    # Build embeddings from prompt_context (what gets searched by user queries)
    texts = [tool["prompt_context"] for tool in tools]
    embeddings = model.encode(texts).tolist()

    points = [
        PointStruct(
            id=i + 1,
            vector=embedding,
            payload={
                "text": tool["prompt_context"],
                "name": tool["name"],
                "description": tool["description"],
                "category": tool["category"],
            },
        )
        for i, (tool, embedding) in enumerate(zip(tools, embeddings))
    ]

    client.upsert(collection_name=COLLECTION_NAME, points=points)
    print(f"Seeded {len(points)} tool prompts into '{COLLECTION_NAME}'")

    # Verify
    info = client.get_collection(COLLECTION_NAME)
    print(f"Collection points count: {info.points_count}")


if __name__ == "__main__":
    main()
