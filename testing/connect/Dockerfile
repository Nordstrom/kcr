FROM openjdk:8 as builder

RUN apt-get update && apt-get install -y git unzip

RUN curl -sL http://archive.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz \
  | gunzip \
  | tar x -C /usr/ \
 && ln -s /usr/apache-maven-3.6.0/bin/mvn /usr/bin/mvn

RUN curl -sL https://services.gradle.org/distributions/gradle-5.2.1-bin.zip -o /tmp/gradle.zip \
 && unzip /tmp/gradle.zip -d /usr/ \
 && ln -s /usr/gradle-5.2.1/bin/gradle /usr/bin/gradle

ENV KAFKA_VERSION=2.1.1

RUN git clone https://github.com/apache/kafka/ /usr/src/kafka \
 && cd /usr/src/kafka \
 && git checkout ${KAFKA_VERSION} \
 && gradle; ./gradlew installAll

# All the Confluent things

ENV CONFLUENT_VERSION=5.1.2

RUN git clone https://github.com/confluentinc/common /usr/src/confluent-common \
 && cd /usr/src/confluent-common \
 && git checkout v${CONFLUENT_VERSION} \
 && mvn install -DskipTests=true

RUN git clone https://github.com/confluentinc/rest-utils /usr/src/rest-utils \
 && cd /usr/src/rest-utils \
 && git checkout v${CONFLUENT_VERSION} \
 && mvn install -DskipTests=true

RUN git clone https://github.com/confluentinc/schema-registry /usr/src/schema-registry \
 && cd /usr/src/schema-registry \
 && git checkout v${CONFLUENT_VERSION} \
 && mvn install -DskipTests=true

RUN git clone https://github.com/confluentinc/kafka-connect-storage-common /usr/src/kafka-connect-storage-common \
 && cd /usr/src/kafka-connect-storage-common \
 && git checkout v${CONFLUENT_VERSION} \
 && mvn install -DskipTests=true

# All the Connectors

ENV S3_CONNECTOR_VERSION=$CONFLUENT_VERSION
ENV SQS_CONNECTOR_VERSION=1.1.0
ENV CONNECT_ASSUME_ROLE_VERSION=1.0.0

# Connector kafka-connect-s3
# Build the connector but throw out the kafka-schema-registry-client that causes
# this issue: https://github.com/confluentinc/schema-registry/issues/1028
# Add the kafka-connect-assume-role jar

RUN git clone https://github.com/confluentinc/kafka-connect-storage-cloud /usr/src/kafka-connect-storage-cloud \
 && cd /usr/src/kafka-connect-storage-cloud \
 && git checkout v${CONFLUENT_VERSION} \
 && mvn package -DskipTests=true \
 && unzip /usr/src/kafka-connect-storage-cloud/kafka-connect-s3/target/components/packages/confluentinc-kafka-connect-s3-${CONFLUENT_VERSION}.zip -d /tmp/ \
 && mv /tmp/confluentinc-kafka-connect-s3-${CONFLUENT_VERSION}/lib /kafka-connect-s3 \
 && rm /kafka-connect-s3/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar \
 && curl -L https://github.com/Nordstrom/kafka-connect-assume-role/releases/download/v${CONNECT_ASSUME_ROLE_VERSION}/kafka-connect-assume-role-${CONNECT_ASSUME_ROLE_VERSION}.jar -o /kafka-connect-s3/kafka-connect-assume-role-${CONNECT_ASSUME_ROLE_VERSION}.jar \
 && chmod 644 /kafka-connect-s3/kafka-connect-assume-role-${CONNECT_ASSUME_ROLE_VERSION}.jar

# Connector kafka-connect-sqs
RUN mkdir -p /kafka-connect-sqs \
 && curl -L https://github.com/Nordstrom/kafka-connect-sqs/releases/download/v${SQS_CONNECTOR_VERSION}/kafka-connect-sqs-${SQS_CONNECTOR_VERSION}.jar -o /kafka-connect-sqs/kafka-connect-sqs-${SQS_CONNECTOR_VERSION}.jar \
 && chmod 644 /kafka-connect-sqs/kafka-connect-sqs-${SQS_CONNECTOR_VERSION}.jar

#
# Kafka Connect-based mirroring tools
#
# Connector MirrorTool
ENV MIRROR_TOOL_VERSION=3.0.1
RUN git clone https://github.com/Comcast/MirrorTool-for-Kafka-Connect.git /usr/src/mirror-tool \
 && cd /usr/src/mirror-tool \
 && git checkout ${MIRROR_TOOL_VERSION} \
 && gradle build \
 && mv build/libs /mirror-tool


FROM confluentinc/cp-kafka-connect-base:5.1.2

RUN groupadd -g 1000 connect \
 && useradd -r -m -u 1000 -g connect connect \
 && sed -i 's/PASSWORD|JAAS_CONFIG/PASSWORD|JAAS_CONFIG|BASIC_AUTH_USER_INFO/' /etc/confluent/docker/bash-config

ENV PROMETHEUS_JMX_EXPORTER_PORT=9011
ENV PROMETHEUS_JMX_EXPORTER_CONF=/etc/jmx_exporter/kafka_connect.yaml
ENV PROMETHEUS_JMX_EXPORTER_VERSION=0.3.1

RUN mkdir -p /etc/jmx_exporter /opt/lib \
 && curl -L "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$PROMETHEUS_JMX_EXPORTER_VERSION/jmx_prometheus_javaagent-$PROMETHEUS_JMX_EXPORTER_VERSION.jar" \
  -o /opt/lib/jmx_prometheus_javaagent.jar \
 && echo "---" > $PROMETHEUS_JMX_EXPORTER_CONF \
 && sed -i "s/^EXTRA_ARGS=.*/EXTRA_ARGS=\${EXTRA_ARGS-'-name connectStandalone -javaagent:\/opt\/lib\/jmx_prometheus_javaagent.jar='\$PROMETHEUS_JMX_EXPORTER_PORT:\$PROMETHEUS_JMX_EXPORTER_CONF}/" /usr/bin/connect-standalone \
 && sed -i "s/^EXTRA_ARGS=.*/EXTRA_ARGS=\${EXTRA_ARGS-'-name connectDistributed -javaagent:\/opt\/lib\/jmx_prometheus_javaagent.jar='\$PROMETHEUS_JMX_EXPORTER_PORT:\$PROMETHEUS_JMX_EXPORTER_CONF}/" /usr/bin/connect-distributed

ENV CONNECT_PLUGIN_PATH /opt/connectors

COPY --from=builder /kafka-connect-s3 ${CONNECT_PLUGIN_PATH}/kafka-connect-s3
COPY --from=builder /kafka-connect-sqs ${CONNECT_PLUGIN_PATH}/kafka-connect-sqs
COPY --from=builder /mirror-tool ${CONNECT_PLUGIN_PATH}/mirror-tool

USER connect
