package org.gbif.pipelines.assembling.pipelines;

public class PipelinePaths {

  private String dataTargetPath;
  private String issuesTargetPath;

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
}
