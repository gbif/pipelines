package org.gbif.pipelines.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventTaxonCoverage implements java.io.Serializable {
  String eventId;
  String taxonCoverage;
}
