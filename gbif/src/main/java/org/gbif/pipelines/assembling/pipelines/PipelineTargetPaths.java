package org.gbif.pipelines.assembling.pipelines;

/**
 * Wrapper for the target paths needed in a {@link org.apache.beam.sdk.Pipeline}.
 */
public class PipelineTargetPaths {

  private String dataTargetPath;
  private String issuesTargetPath;

  String getDataTargetPath() {
    return dataTargetPath;
  }

  String getIssuesTargetPath() {
    return issuesTargetPath;
  }

  void setDataTargetPath(String dataTargetPath) {
    this.dataTargetPath = dataTargetPath;
  }

  void setIssuesTargetPath(String issuesTargetPath) {
    this.issuesTargetPath = issuesTargetPath;
  }
}
