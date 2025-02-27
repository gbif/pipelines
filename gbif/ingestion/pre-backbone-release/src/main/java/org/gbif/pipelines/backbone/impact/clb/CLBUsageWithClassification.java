package org.gbif.pipelines.backbone.impact.clb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CLBUsageWithClassification extends CLBUsage implements Serializable {
  List<CLBUsage> classification;
}
