FROM grafana/grafana:latest

USER root

# Copy the plugins
COPY trino-datasource /var/lib/grafana/plugins/trino-datasource
COPY haohanyang-mongodb-datasource /var/lib/grafana/plugins/haohanyang-mongodb-datasource

# Set execute permissions on plugin binaries
RUN chmod +x /var/lib/grafana/plugins/haohanyang-mongodb-datasource/gpx_mongodb_datasource_* || true && \
    chmod +x /var/lib/grafana/plugins/trino-datasource/gpx_* || true

# Fix ownership
RUN chown -R 472:472 /var/lib/grafana/plugins

# Set environment for unsigned plugins
ENV GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=haohanyang-mongodb-datasource,trino-datasource

USER 472
