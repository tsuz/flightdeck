 # 1. Start Qdrant
  docker compose up -d

  # 2. Install Python deps
  pip install -r requirements.txt

  # 3. Seed documents
  python seed.py

  # 4. Test a query
  python query.py "schedule a meeting with John"

  # 5. Start the RAG server (think-consumer calls this)
  python rag_server.py

  The RAG server listens on http://localhost:8081/api/rag/query which is the default RAG_API_URL in the think-consumer's config.