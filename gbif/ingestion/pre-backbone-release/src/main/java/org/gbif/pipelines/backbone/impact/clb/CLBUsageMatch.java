package org.gbif.pipelines.backbone.impact.clb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CLBUsageMatch {
  CLBUsageWithClassification usage;
  String type;
  String status;
  String id;
  boolean match;
}
