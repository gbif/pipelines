package org.gbif.pipelines.io.avro;

public interface Issues {
  void setIssues(IssueRecord issues);

  default IssueRecord getIssues() {
    return IssueRecord.newBuilder().build();
  }
}
