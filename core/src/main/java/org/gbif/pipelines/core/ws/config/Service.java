package org.gbif.pipelines.core.ws.config;

/**
 * Enum of web services supported in the application.
 */
public enum Service {
  SPECIES_MATCH2("match"), GEO_CODE("geocode"),GBIF_INTERNAL("internal");

  // path to use in the properties file
  private String path;

  Service(String path) {
    this.path = path;
  }

  public String getPath() {
    return this.path;
  }

}