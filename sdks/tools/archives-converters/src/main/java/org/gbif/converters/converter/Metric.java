package org.gbif.converters.converter;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(staticName = "create")
public class Metric {

  private final long numberOfRecords;
  private final long numberOfOccurrenceRecords;
  private final Map<String, Long> extensionsCount;
}
