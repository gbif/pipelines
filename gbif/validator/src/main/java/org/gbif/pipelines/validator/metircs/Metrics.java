package org.gbif.pipelines.validator.metircs;

import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Builder
@Data
@ToString
public class Metrics {

  private Map<String, Long> coreTermsCountMap;
  private Map<String, Map<String, Long>> extensionsTermsCountMap;
  private Map<String, Long> occurrenceIssuesMap;
}
