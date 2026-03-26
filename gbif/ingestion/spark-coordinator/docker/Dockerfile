FROM spark:3.5.1-scala2.12-java17-python3-ubuntu
ARG JAR_FILE=${JAVA_JAR_FILE}
ARG JMX_EXPORTER_VERSION=1.0.1
USER root
RUN mkdir /jobs
COPY --chown=1000:1000 "./target/${JAR_FILE}" "/jobs/ingest-spark-coordinator.jar"

# Add Iceberg runtime jar to Spark jars folder
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-extensions-3.5_2.12/1.6.1/iceberg-spark-extensions-3.5_2.12-1.6.1.jar \
    /opt/spark/jars/

# Download JMX exporter
RUN chmod 777 /opt/spark/jars/iceberg-spark-extensions-3.5_2.12-1.6.1.jar
RUN chmod 777 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
RUN chmod 777 /opt/spark/work-dir
# Install tini so we have a proper PID 1 that reaps children and forwards signals
RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*

RUN groupadd -r stackable && useradd -r -g stackable -m stackable
USER stackable:stackable

# Ensure Spark jars are accessible to the JVM
ENV SPARK_HOME=/opt/spark
ENV JVM_OPTIONS="-XX:+CrashOnOutOfMemoryError -XX:+PerfDisableSharedMem -XX:+UseShenandoahGC -Xms8G -Xmx12G"

# Default runtime arguments
CMD ["MODE", "PATH_TO_CONFIG", "1"]

# Prometheus metrics port
EXPOSE 9404
ENTRYPOINT ["/usr/bin/tini", "--", "sh", "-c", "exec java $JVM_OPTIONS \
  -DpipelinesVersion $VERSION \
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports java.base/sun.security.action=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens java.base/java.net=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  -cp /opt/spark/jars/*:/jobs/ingest-spark-coordinator.jar \
  org.gbif.pipelines.coordinator.Coordinator \"$@\"", "--"]
