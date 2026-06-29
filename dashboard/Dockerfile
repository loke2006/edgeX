FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dashboard/ .

# Collect static files
RUN python manage.py collectstatic --noinput 2>/dev/null || true

# Create non-root user
RUN adduser --disabled-password --no-create-home --gecos "" appuser
USER appuser

# Expose port
EXPOSE 8000

# Run with Daphne (ASGI server for Channels)
CMD ["daphne", "-b", "0.0.0.0", "-p", "8000", "edgecloudx.asgi:application"]
