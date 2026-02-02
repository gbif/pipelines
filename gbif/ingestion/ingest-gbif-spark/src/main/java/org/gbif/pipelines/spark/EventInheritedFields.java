package org.gbif.pipelines.spark;

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
public class EventInheritedFields {

  String id;
  String locationInherited;
  String temporalInherited;
  String eventInherited;
}
