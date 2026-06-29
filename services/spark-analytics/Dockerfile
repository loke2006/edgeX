FROM apache/spark:3.5.1

USER root

# Install Python dependencies
COPY services/spark-analytics/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY services/spark-analytics/main.py /app/main.py

# Create and switch to non-root user (or use spark's default 185)
# Apache Spark container defaults to user 185, which works perfectly.
USER 185

WORKDIR /app

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1", \
     "/app/main.py"]
