FROM python:3.11-slim

WORKDIR /app

# Install CPU-only PyTorch (smaller image, sufficient for inference)
RUN pip install --no-cache-dir torch==2.3.1+cpu --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir fastapi==0.115.0 uvicorn[standard]==0.30.6 transformers==4.44.2 pydantic==2.8.2

COPY main.py .

ENV MODEL_CACHE=/app/models

# Pre-download model at build time so container starts instantly
RUN python -c "from transformers import AutoTokenizer, AutoModel; \
    AutoTokenizer.from_pretrained('anferico/bert-for-patents', cache_dir='/app/models'); \
    AutoModel.from_pretrained('anferico/bert-for-patents', cache_dir='/app/models')"

EXPOSE 8002
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002", "--workers", "1"]
