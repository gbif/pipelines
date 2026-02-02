package org.gbif.pipelines.spark;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO that holds the contents of an Occurrence. Each string property is JSON serialisation of the
 * output from a transform.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Occurrence {
  String id;
  String coreId;
  String internalId;
  String verbatim;
  String identifier;
  String basic;
  String location;
  String taxon;
  String temporal;
  String grscicoll;
  String dnaDerivedData;
  String multimedia;
  String audubon;
  String image;
  String clustering;
}
