FROM eclipse-temurin:21-jre-alpine

ARG JAR_FILE=${JAVA_JAR_FILE}
USER root
RUN mkdir /jobs
COPY --chown=1000:1000 "./target/${JAR_FILE}" "/jobs/healthcheck.jar"

EXPOSE 8080

CMD ["java", "-jar", "/jobs/healthcheck.jar"]