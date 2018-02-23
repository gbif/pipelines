package org.gbif.pipelines.core.config;

/***
 * Definition of recognized outputs of the data pipeline.
 */
public enum RecordInterpretation {

  RAW_OCCURRENCE("raw_data"), INTERPRETED_OCURENCE("interpreted_data"), VERBATIM("verbatim"), TEMPORAL("temporal"), LOCATION(
    "location"), GBIF_BACKBONE("gbif-backbone"), TEMP_DwCA_PATH("temp"), TEMPORAL_ISSUE("temporal_issue"), LOCATION_ISSUE(
    "location_issue"), INTERPRETED_ISSUE("interpreted_issue"), ISSUES("issues"), RECORD_LEVEL("interpreted");

  private final String defaultFileName;

  /**
   * Default constructor: receives the name of the output file or directory.
   *
   * @param defaultFileName default output file name
   */
  RecordInterpretation(String defaultFileName) {
    this.defaultFileName = defaultFileName;
  }

  /**
   * @return default file name
   */
  public String getDefaultFileName() {
    return defaultFileName;
  }

}
