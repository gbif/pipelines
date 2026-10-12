# syntax=docker/dockerfile:1.4
ARG BASE_REGISTRY=third-party
FROM ${BASE_REGISTRY}/eclipse-temurin:17-jre

ARG JAR_FILE
LABEL authors="gbif"

RUN useradd -r -s /bin/false stackable
WORKDIR /app

COPY --chown=stackable ${JAR_FILE} /app/service.jar
COPY --chown=stackable docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV RABBITMQ_HOST=localhost \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USERNAME=guest \
    RABBITMQ_PASSWORD=guest \
    RABBITMQ_VHOST=/ \
    VALIDATOR_WS_URL=http://localhost:8080 \
    VALIDATOR_WS_USERNAME="" \
    VALIDATOR_WS_PASSWORD="" \
    ORCHESTRATOR_UNSAFE_ENABLE_START_CONSUMER=false \
    ORCHESTRATOR_UNSAFE_ENABLE_COMPLETION_CONSUMER=false \
    JVM_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Xms128m"

USER stackable
ENTRYPOINT ["/entrypoint.sh"]
