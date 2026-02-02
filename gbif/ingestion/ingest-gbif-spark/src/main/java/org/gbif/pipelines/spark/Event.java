package org.gbif.pipelines.spark;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO that holds the contents of an Event. Each string property is JSON serialisation of the
 * output from a transform.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {

  String id;
  List<String> lineage;
  String internalId;
  String verbatim;
  String identifier;
  String location;
  String taxon;
  String temporal;
  String multimedia;
  String audubon;
  String image;
  String eventCore;
  String measurementOrFact;
  String humboldt;

  // inherited records
  String locationInherited;
  String temporalInherited;
  String eventInherited;

  // derived
  String derivedMetadata;
}
