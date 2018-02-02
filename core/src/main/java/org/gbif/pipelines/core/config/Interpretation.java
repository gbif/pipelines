package org.gbif.pipelines.core.config;

/***
 * Definition of recognized outputs of the data pipeline.
 */
public enum Interpretation {

  VERBATIM("verbatim"), TEMPORAL("temporal"), LOCATION("location"), GBIF_BACKBONE("gbif-backbone"), ISSUES("issues"),
  RECORD_LEVEL("interpreted");

  private final String defaultFileName;

  /**
   * Default constructor: receives the name of the output file or directory.
   * @param defaultFileName default output file name
   */
  Interpretation(String defaultFileName) {
    this.defaultFileName = defaultFileName;
  }

  /**
   *
   * @return default file name
   */
  public String getDefaultFileName() {
    return defaultFileName;
  }

}
