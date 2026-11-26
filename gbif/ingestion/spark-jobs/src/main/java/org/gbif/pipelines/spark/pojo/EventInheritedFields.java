package org.gbif.pipelines.spark.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

/** POJO that holds the values an Event inherits from its parent events, as typed records. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventInheritedFields {

  String id;
  LocationInheritedRecord locationInherited;
  TemporalInheritedRecord temporalInherited;
  EventInheritedRecord eventInherited;
}
