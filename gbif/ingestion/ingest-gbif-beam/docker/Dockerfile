FROM alpine:latest
RUN apk update && \
    apk upgrade && \
    apk add bash && \
    rm -rf /var/cache/apk/*
RUN mkdir /jobs
COPY ../target/ingest-gbif-beam-2.14.0-spark3-SNAPSHOT.jar /jobs/gbif-ingest.jar

