FROM solr:8.11.2

USER 1001
ADD https://repo1.maven.org/maven2/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar \
    /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/jts-core.jar

ADD https://repo1.maven.org/maven2/org/locationtech/jts/io/jts-io-common/1.19.0/jts-io-common-1.19.0.jar \
    /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/jts-io-common.jar

USER root
RUN chmod 755 /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/jts-io-common.jar
RUN chmod 755 /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/jts-core.jar
USER solr
