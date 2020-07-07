package org.gbif.pipelines.common.configs;

public interface BaseConfiguration {

  String getHdfsSiteConfig();

  String getCoreSiteConfig();

  String getRepositoryPath();

  String getMetaFileName();
}
