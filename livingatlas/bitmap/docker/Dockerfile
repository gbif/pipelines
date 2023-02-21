FROM postgis/postgis:13-master

RUN apt-get update && \
      apt-get install -y --no-install-recommends \
      postgis ca-certificates curl \
      && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /tmp/jdk8.tgz https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u332-b09/OpenJDK8U-jre_x64_linux_hotspot_8u332b09.tar.gz

RUN mkdir -p /usr/lib/jvm/

RUN tar xfz /tmp/jdk8.tgz --directory /usr/lib/jvm/

VOLUME ["/data/pipelines-shp"]

COPY target/bitmap-2.14.0-SNAPSHOT-jar-with-dependencies.jar /usr/local/lib/la-pipelines-bitmap.jar
