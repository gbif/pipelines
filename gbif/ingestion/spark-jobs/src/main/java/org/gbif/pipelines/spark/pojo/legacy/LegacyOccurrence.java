package org.gbif.pipelines.spark.pojo.legacy;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The pre-typed shape of {@code simple_occurrence}, where each transform output was stored as a
 * JSON string. Only used to read Parquet written by earlier pipelines versions; see {@link
 * org.gbif.pipelines.spark.util.SimpleRecordLoader}. Remove once no retained attempt uses it.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LegacyOccurrence {
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
