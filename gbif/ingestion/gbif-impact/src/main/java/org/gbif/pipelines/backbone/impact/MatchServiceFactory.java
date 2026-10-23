package org.gbif.pipelines.backbone.impact;

import java.io.Serializable;
import org.gbif.rest.client.RestClientFactory;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatchingService;

/** Creates the name matching service on the executors. */
@FunctionalInterface
public interface MatchServiceFactory extends Serializable {

  NameUsageMatchingService create();

  /** A direct service against the API, no cache. */
  static MatchServiceFactory rest(String baseApiUrl) {
    return () ->
        RestClientFactory.createNameMatchService(
            ClientConfiguration.builder()
                .withBaseApiUrl(baseApiUrl)
                .withFileCacheMaxSizeMb(1L)
                .withTimeOutMillisec(120_000)
                .build());
  }
}
