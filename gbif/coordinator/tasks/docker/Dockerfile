FROM docker.gbif.org/gbif-builder-image:0.1.0 as builder
WORKDIR /home/gbif
COPY gbif ./gbif
COPY sdks ./sdks
COPY pom.xml .
RUN mvn -f pom.xml clean package install -Dmaven.test.skip=true -T 2 -P gbif-docker,gbif-artifacts,extra-artifacts

FROM docker.gbif.org/gbif-base-image:0.2.0
WORKDIR /home/gbif
COPY --from=builder /home/gbif/lib ./lib
COPY --from=builder /home/gbif/tasks-coordinator-cli-2.15.0-H3-SNAPSHOT.jar ./pipelines-coordinator-cli-2.15.0-H3-SNAPSHOT.jar
