FROM bitnami/spark:3.5

USER root

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application
COPY main.py /app/main.py

# Create non-root user
RUN useradd -m -s /bin/bash sparkuser
USER sparkuser

WORKDIR /app

CMD ["spark-submit", \
     "--master", "local[2]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1", \
     "/app/main.py"]
