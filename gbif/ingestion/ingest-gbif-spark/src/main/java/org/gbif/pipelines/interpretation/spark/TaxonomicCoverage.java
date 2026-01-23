package org.gbif.pipelines.interpretation.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaxonomicCoverage implements java.io.Serializable {
  String eventId;
  String classifications;
}
