package org.gbif.pipelines.spark.pojo;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

/**
 * POJO that holds the contents of an interpreted Event. Each property holds the typed output of one
 * transform, written to Parquet as a nested struct.
 *
 * <p>This class defines the published {@code event} Parquet schema (see {@link
 * org.gbif.pipelines.spark.schema.ParquetSchemas}). Changing a field changes that schema: follow
 * the versioning rules there and regenerate the checked-in schema files.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {

  String id;
  List<String> lineage;
  String internalId;
  ExtendedRecord verbatim;
  IdentifierRecord identifier;
  LocationRecord location;
  TemporalRecord temporal;
  MultimediaRecord multimedia;
  AudubonRecord audubon;
  ImageRecord image;
  EventCoreRecord eventCore;
  MeasurementOrFactRecord measurementOrFact;
  HumboldtRecord humboldt;

  // inherited records
  LocationInheritedRecord locationInherited;
  TemporalInheritedRecord temporalInherited;
  EventInheritedRecord eventInherited;

  // derived
  DerivedMetadataRecord derivedMetadata;
}
