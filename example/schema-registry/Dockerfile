FROM confluentinc/cp-schema-registry:5.1.2

RUN groupadd -g 1000 schema-registry \
 && useradd -r -m -u 1000 -g schema-registry schema-registry

ENV PROMETHEUS_JMX_EXPORTER_PORT=9011
ENV PROMETHEUS_JMX_EXPORTER_CONF=/etc/jmx_exporter/schema_registry.yaml
ENV PROMETHEUS_JMX_EXPORTER_VERSION=0.11.0

RUN mkdir -p /etc/jmx_exporter /opt/lib \
 && curl -L "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$PROMETHEUS_JMX_EXPORTER_VERSION/jmx_prometheus_javaagent-$PROMETHEUS_JMX_EXPORTER_VERSION.jar" \
  -o /opt/lib/jmx_prometheus_javaagent.jar \
 && echo "---" > $PROMETHEUS_JMX_EXPORTER_CONF \
 && sed -i "s/^EXTRA_ARGS=.*/EXTRA_ARGS=\${EXTRA_ARGS-'-name schemaRegistry -javaagent:\/opt\/lib\/jmx_prometheus_javaagent.jar='\$PROMETHEUS_JMX_EXPORTER_PORT:\$PROMETHEUS_JMX_EXPORTER_CONF}/" /usr/bin/schema-registry-start

USER schema-registry
