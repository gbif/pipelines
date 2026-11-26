package org.gbif.pipelines.spark.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

/**
 * POJO that holds the contents of an interpreted Occurrence. Each property holds the typed output
 * of one transform, written to Parquet as a nested struct.
 *
 * <p>This class defines the published {@code occurrence} Parquet schema (see {@link
 * org.gbif.pipelines.spark.schema.ParquetSchemas}). Changing a field changes that schema: follow
 * the versioning rules there and regenerate the checked-in schema files.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Occurrence {
  String id;
  String coreId;
  String internalId;
  ExtendedRecord verbatim;
  IdentifierRecord identifier;
  BasicRecord basic;
  LocationRecord location;
  MultiTaxonRecord taxon;
  TemporalRecord temporal;
  GrscicollRecord grscicoll;
  DnaDerivedDataRecord dnaDerivedData;
  MultimediaRecord multimedia;
  AudubonRecord audubon;
  ImageRecord image;
  ClusteringRecord clustering;
}
