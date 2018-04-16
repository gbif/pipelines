package org.gbif.pipelines.assembling.interpretation.steps;

/**
 * Wrapper for the target paths needed in a {@link org.apache.beam.sdk.Pipeline}.
 */
public class PipelineTargetPaths {

  private String dataTargetPath;
  private String issuesTargetPath;
  private String tempDir;

  public String getDataTargetPath() {
    return dataTargetPath;
  }

  public String getIssuesTargetPath() {
    return issuesTargetPath;
  }

  public void setDataTargetPath(String dataTargetPath) {
    this.dataTargetPath = dataTargetPath;
  }

  public void setIssuesTargetPath(String issuesTargetPath) {
    this.issuesTargetPath = issuesTargetPath;
  }

  public String getTempDir() {
    return tempDir;
  }

  public void setTempDir(String tempDir) {
    this.tempDir = tempDir;
  }
}
