package org.gbif.pipelines.parsers.config.factory;

import java.nio.file.Path;
import java.util.Properties;

import org.gbif.pipelines.parsers.config.model.ElasticsearchContentConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
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

  @VisibleForTesting
  public static final String CONTENTFUL_ELASTICSEARCH = "content.es";

  public static ElasticsearchContentConfig create(@NonNull Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    return create(props);
  }

  public static ElasticsearchContentConfig create(@NonNull Properties props) {
    // get the base path or throw exception if not present
    String property = props.getProperty(CONTENTFUL_ELASTICSEARCH);
    if(Strings.isNullOrEmpty(property)){
      throw new IllegalArgumentException("Property " + CONTENTFUL_ELASTICSEARCH + " can't be null or emtry, check the property file");
    }
    String[] hosts = property.split(",");
    return ElasticsearchContentConfig.create(hosts);
  }
}
