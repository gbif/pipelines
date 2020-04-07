package org.gbif.pipelines.common.configs;

public interface BaseConfiguration {

  String getHdfsSiteConfig();

  String getRepositoryPath();

  String getMetaFileName();
}
