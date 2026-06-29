FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libgl1 \
    libglib2.0-0t64 \
    && rm -rf /var/lib/apt/lists/*

COPY services/edge-node/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy shared modules
COPY shared/ /app/shared/

# Copy application code
COPY services/edge-node/ .

# Create non-root user
RUN adduser --disabled-password --no-create-home --gecos "" appuser
USER appuser

CMD ["python", "main.py"]
