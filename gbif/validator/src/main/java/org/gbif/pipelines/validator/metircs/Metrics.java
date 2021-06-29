package org.gbif.pipelines.validator.metircs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Data;
import org.gbif.pipelines.validator.ValidationStatus;

@JsonDeserialize(builder = Metrics.MetricsBuilder.class)
@Builder
@Data
public class Metrics {

  @Builder.Default private String datasetKey = UUID.randomUUID().toString();
  @Builder.Default private Long startTimestamp = ZonedDateTime.now().toEpochSecond();
  private Long endTimestamp;
  @Builder.Default private ValidationStatus status = ValidationStatus.RUNNING;
  @Builder.Default private Result result = Result.builder().build();

  @JsonDeserialize(builder = Result.ResultBuilder.class)
  @Builder
  @Data
  public static class Result {
    @Builder.Default private Core core = Core.builder().build();
    @Builder.Default private List<Extension> extensions = Collections.emptyList();
  }

  @JsonDeserialize(builder = Core.CoreBuilder.class)
  @Builder
  @Data
  public static class Core {
    @Builder.Default private Long fileCount = 0L;
    @Builder.Default private Long indexedCount = 0L;
    @Builder.Default private Map<String, TermInfo> indexedCoreTerms = Collections.emptyMap();
    @Builder.Default private Map<String, Long> occurrenceIssues = Collections.emptyMap();

    @JsonDeserialize(builder = TermInfo.TermInfoBuilder.class)
    @Builder
    @Data
    public static class TermInfo {
      @Builder.Default private Long rawIndexed = 0L;
      @Builder.Default private Long interpretedIndexed = null;
    }
  }

  @JsonDeserialize(builder = Extension.ExtensionBuilder.class)
  @Builder
  @Data
  public static class Extension {
    @Builder.Default private String rowType = "";
    @Builder.Default private Long fileCount = 0L;
    @Builder.Default private Map<String, Long> extensionsTermsCounts = Collections.emptyMap();
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
