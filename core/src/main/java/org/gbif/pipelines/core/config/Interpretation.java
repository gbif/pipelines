package org.gbif.pipelines.core.config;

public enum Interpretation {

  VERBATIM("verbatim"), TEMPORAL("temporal"), LOCATION("location"), GBIF_BACKBONE("gbif-backbone");

  private String defaultFileName;

  Interpretation(String defaultFileName) {
    this.defaultFileName = defaultFileName;
  }

  public String getDefaultFileName() {
    return defaultFileName;
  }

}
