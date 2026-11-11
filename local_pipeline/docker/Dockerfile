# Image for the event producer and the local pipeline.
# Build context is the repository root (see docker-compose.yml).
FROM python:3.11-slim-bookworm

# Spark 3.5 runs on Java 17.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# Start Spark once so it downloads the Delta and Kafka libraries into the
# image. Containers then start without needing Maven Central.
RUN python -m local_pipeline.spark_session

CMD ["python", "-m", "local_pipeline.run_pipeline", "--help"]
