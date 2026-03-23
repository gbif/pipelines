package org.gbif.pipelines.spark.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class EventTaxonCoverage implements java.io.Serializable {
  String eventId;
  String taxonCoverage;
}
