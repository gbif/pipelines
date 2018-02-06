package org.gbif.pipelines.core.config;

public enum Interpretation {

  RAW_OCCURRENCE("raw_data"), INTERPRETED_OCURENCE("interpreted"), VERBATIM("verbatim"), TEMPORAL("temporal"), LOCATION(
    "location"), GBIF_BACKBONE("gbif-backbone"), TEMP_DwCA_PATH("temp"), TEMPORAL_ISSUE("temporal_issue"), LOCATION_ISSUE(
    "location_issue"), INTERPRETED_ISSUE("interpreted_issue");

  private String defaultFileName;

  Interpretation(String defaultFileName) {
    this.defaultFileName = defaultFileName;
  }

  public String getDefaultFileName() {
    return defaultFileName;
  }

}
