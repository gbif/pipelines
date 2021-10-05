package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.validator.api.Validation.Status;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonDeserialize(builder = Metrics.MetricsBuilder.class)
public class Metrics {

  @Builder.Default private List<ValidationStep> stepTypes = new ArrayList<>();

  @JsonProperty("files")
  @Builder.Default
  private List<FileInfo> fileInfos = new ArrayList<>();

  private String error;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = ValidationStep.ValidationStepBuilder.class)
  public static class ValidationStep {

    /** Specific Validation StepType to avoid gbif-api dependency. */
    public enum StepType {
      VALIDATOR_UPLOAD_ARCHIVE("validatorUploadArchive", 1),
      VALIDATOR_VALIDATE_ARCHIVE("validatorValidateArchive", 2),
      VALIDATOR_DWCA_TO_VERBATIM("validatorDwcaToVerbatim", 3),
      VALIDATOR_XML_TO_VERBATIM("validatorXmlToVerbatim", 3),
      VALIDATOR_ABCD_TO_VERBATIM("validatorAbcdToVerbatim", 4),
      VALIDATOR_VERBATIM_TO_INTERPRETED("validatorVerbatimToInterpreted", 4),
      VALIDATOR_INTERPRETED_TO_INDEX("validatorInterpretedToIndex", 5),
      VALIDATOR_COLLECT_METRICS("validatorCollectMetrics", 6);

      private String label;
      private int executionOrder;

      StepType(String label, int executionOrder) {
        this.label = label;
        this.executionOrder = executionOrder;
      }

      public int getExecutionOrder() {
        return executionOrder;
      }

      public String getLabel() {
        return label;
      }
    }

    private StepType stepType;
    private Status status;
    private String message;
    private int executionOrder;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = FileInfo.FileInfoBuilder.class)
  public static class FileInfo {
    private String fileName;
    private DwcFileType fileType;
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
    private String extra;
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
