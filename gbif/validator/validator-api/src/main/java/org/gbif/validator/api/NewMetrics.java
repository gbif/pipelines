package org.gbif.validator.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation.Status;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonDeserialize(builder = NewMetrics.NewMetricsBuilder.class)
public class NewMetrics {

  @Builder.Default private List<ValidationStep> stepTypes = new LinkedList<>();
  @Builder.Default private List<FileInfo> files = new ArrayList<>();

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = ValidationStep.ValidationStepBuilder.class)
  public static class ValidationStep {
    private StepType stepType;
    private Status status;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = FileInfo.FileInfoBuilder.class)
  public static class FileInfo {
    private String fileName;
    private String fileType;
    private Long count;
    private Long indexedCount;
    private String rowType;
    @Builder.Default private List<TermInfo> terms = Collections.emptyList();
    @Builder.Default private List<IssueInfo> issues = Collections.emptyList();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = TermInfo.TermInfoBuilder.class)
  public static class TermInfo {
    private String term;
    private Long rawIndexed;
    private Long interpretedIndexed;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = IssueInfo.IssueInfoBuilder.class)
  public static class IssueInfo {
    private String issue;
    private EvaluationCategory issueCategory;
    private Long count;
    @Builder.Default private List<IssueSample> samples = Collections.emptyList();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = IssueSample.IssueSampleBuilder.class)
  public static class IssueSample {
    private String recordId;
    private Map<String, String> relatedData = Collections.emptyMap();
  }

  @Override
  public String toString() {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(this);
    } catch (IOException e) {
      // NOP
    }
    return "";
  }
}
