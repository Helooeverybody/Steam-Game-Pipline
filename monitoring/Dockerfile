FROM ajeje93/grafana-mongodb:latest

USER root

# Copy the trino plugin
COPY trino-datasource /var/lib/grafana/plugins/trino-datasource

# Fix permissions (using numeric UID:GID 472:472 which is standard for Grafana)
RUN chown -R 472:472 /var/lib/grafana/plugins

USER 472
