"""
Seeds Qdrant with the architecture and troubleshooting documents,
split into chunks for semantic search by the RAG service.

Usage:
    python seed.py
"""

import os
import re
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer

COLLECTION_NAME = "prompts"
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
VECTOR_SIZE = 384

DOCS_DIR = Path(os.environ.get("DOCS_DIR", Path(__file__).parent.parent / "docs"))


def chunk_markdown(text, source):
    """Split markdown into sections based on ## headings."""
    chunks = []
    sections = re.split(r'\n(?=## )', text)

    for section in sections:
        section = section.strip()
        if not section or len(section) < 20:
            continue
        # Extract heading for metadata
        lines = section.split('\n', 1)
        heading = lines[0].lstrip('#').strip()
        chunks.append({
            "text": section,
            "source": source,
            "heading": heading,
        })

    return chunks


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

    # Load and chunk all markdown docs
    all_chunks = []
    for md_file in sorted(DOCS_DIR.glob("*.md")):
        print(f"Loading {md_file.name}...")
        text = md_file.read_text()
        chunks = chunk_markdown(text, md_file.name)
        all_chunks.extend(chunks)
        print(f"  → {len(chunks)} chunks")

    if not all_chunks:
        print("No documents found!")
        return

    # Embed and upsert
    texts = [c["text"] for c in all_chunks]
    embeddings = model.encode(texts).tolist()

    points = [
        PointStruct(
            id=i + 1,
            vector=embedding,
            payload={
                "text": chunk["text"],
                "source": chunk["source"],
                "heading": chunk["heading"],
                "category": "ops",
            },
        )
        for i, (chunk, embedding) in enumerate(zip(all_chunks, embeddings))
    ]

    client.upsert(collection_name=COLLECTION_NAME, points=points)
    print(f"\nSeeded {len(points)} chunks into '{COLLECTION_NAME}'")

    info = client.get_collection(COLLECTION_NAME)
    print(f"Collection points count: {info.points_count}")


if __name__ == "__main__":
    main()
