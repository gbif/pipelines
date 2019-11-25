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

  public static final String CONTENTFUL_PREFIX = "contentful";

  public static final String SPACE_ID_PROP = ".spaceId";

  public static final String AUTH_TOKEN_PROP = ".auth_token";

  public static ContentfulConfig create(@NonNull Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    return create(props);
  }


  public static ContentfulConfig create(@NonNull Properties props) {
    // get the base path or throw exception if not present
    String spaceId = props.getProperty(CONTENTFUL_PREFIX + SPACE_ID_PROP);
    String authToken = props.getProperty(CONTENTFUL_PREFIX + AUTH_TOKEN_PROP);

    return ContentfulConfig.create(spaceId, authToken);
  }
}
