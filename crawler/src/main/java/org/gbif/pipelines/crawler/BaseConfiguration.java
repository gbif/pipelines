package org.gbif.pipelines.crawler;

public interface BaseConfiguration {

  String getHdfsSiteConfig();

  String getRepositoryPath();

  String getMetaFileName();

}
