package org.gbif.pipelines.parsers.config;

import java.nio.file.Path;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Creates the configuration for Contentful access.
 *
 * <p>By default it reads the configuration from the "http.properties" file.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ContentfulConfigFactory {

  private static final String CONTENTFUL_ELASTICSEARCH = "content.es";


  public static ElasticsearchContentConfig create(@NonNull Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    return create(props);
  }


  public static ElasticsearchContentConfig create(@NonNull Properties props) {
    // get the base path or throw exception if not present
    String[] hosts = props.getProperty(CONTENTFUL_ELASTICSEARCH).split(",");
    return ElasticsearchContentConfig.create(hosts);
  }
}
