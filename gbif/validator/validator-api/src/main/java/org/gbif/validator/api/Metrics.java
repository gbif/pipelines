package org.gbif.validator.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.api.model.crawler.GenericValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation.Status;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonDeserialize(builder = Metrics.MetricsBuilder.class)
public class Metrics {

  @Builder.Default private Map<StepType, Status> stepTypes = new HashMap<>();
  @Builder.Default private Core core = Core.builder().build();
  @Builder.Default private List<Extension> extensions = Collections.emptyList();
  private ChecklistValidationReport checklistValidationReport;

  @Builder.Default
  private ArchiveValidationReport archiveValidationReport =
      ArchiveValidationReport.builder().build();

  @Builder.Default
  private XmlSchemaValidatorResult xmlSchemaValidatorResult =
      XmlSchemaValidatorResult.builder().build();

  public void addStepType(StepType stepType, Status status) {
    stepTypes.put(stepType, status);
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = Core.CoreBuilder.class)
  public static class Core {
    @Builder.Default private Long fileCount = 0L;
    @Builder.Default private Long indexedCount = 0L;

    @Builder.Default private Map<String, TermInfo> indexedCoreTerms = Collections.emptyMap();

    @Builder.Default private Map<String, Long> occurrenceIssues = Collections.emptyMap();

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonDeserialize(builder = Core.TermInfo.TermInfoBuilder.class)
    public static class TermInfo {
      @Builder.Default private Long rawIndexed = 0L;
      @Builder.Default private Long interpretedIndexed = null;
    }
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = Extension.ExtensionBuilder.class)
  public static class Extension {
    @Builder.Default private String rowType = "";
    @Builder.Default private Long fileCount = null;
    @Builder.Default private Map<String, Long> extensionsTermsCounts = Collections.emptyMap();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = ArchiveValidationReport.ArchiveValidationReportBuilder.class)
  public static class ArchiveValidationReport {
    private OccurrenceValidationReport occurrenceReport;
    private GenericValidationReport genericReport;
    private String invalidationReason;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonDeserialize(builder = ChecklistValidationReport.ChecklistValidationReportBuilder.class)
  public static class ChecklistValidationReport {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonDeserialize(builder = ChecklistValidationResult.ChecklistValidationResultBuilder.class)
    public static class ChecklistValidationResult {
      private NameUsage nameUsage;
      private VerbatimNameUsage verbatimNameUsage;
    }

    private List<ChecklistValidationResult> results;
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
