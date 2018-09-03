package org.gbif.pipelines.parsers.ws.config;

/** Enum of web services supported in the application. */
public enum ServiceType {
  SPECIES_MATCH2("match"),
  GEO_CODE("geocode"),
  DATASET_META("metads");

  // path to use in the properties file
  private final String path;

  ServiceType(String path) {
    this.path = path;
  }

  public String getPath() {
    return this.path;
  }
}
