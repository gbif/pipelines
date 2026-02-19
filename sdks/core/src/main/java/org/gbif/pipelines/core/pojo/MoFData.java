package org.gbif.pipelines.core.pojo;

import java.util.HashSet;
import java.util.Set;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MoFData {

  @Builder.Default Set<String> measurementTypes = new HashSet<>();
  @Builder.Default Set<String> measurementTypeIDs = new HashSet<>();
}
