package org.gbif.pipelines.core.pojo;

import java.util.HashSet;
import java.util.Set;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MoFData {

  Set<String> measurementTypes = new HashSet<>();
  Set<String> measurementTypeIDs = new HashSet<>();
}
