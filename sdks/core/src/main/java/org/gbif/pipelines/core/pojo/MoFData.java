package org.gbif.pipelines.core.pojo;

import lombok.Builder;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
@Builder
public class MoFData {

  Set<String> measurementTypes = new HashSet<>();
  Set<String> measurementTypeIDs = new HashSet<>();

}
