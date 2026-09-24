# Flink 1.20 with the connectors our job needs. Fluss 1.0 supports Flink 1.20 (fluss-flink-1.20).
FROM flink:1.20.3-scala_2.12-java17

# Connectors: Fluss (the job's sink and the price_gap source), Kafka (reads processed-data).
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/1.0.0/fluss-flink-1.20-1.0.0.jar /opt/flink/lib/
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/

# Metrics: the Prometheus reporter is already in plugins/metrics-prometheus; flink.yaml turns it on.

# The loop that keeps the job running. The job's SQL comes from a ConfigMap (k8s/flink-live.sql).
COPY --chown=flink:flink submit.sh /opt/crypto/
RUN chmod +x /opt/crypto/submit.sh
