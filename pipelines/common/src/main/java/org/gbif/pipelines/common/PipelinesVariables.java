package org.gbif.pipelines.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Simple class with constants, general idea to have clean jar with constant only
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesVariables {

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Pipeline {

    public static final String AVRO_EXTENSION = ".avro";

    public static final String DWCA_TO_VERBATIM = "dwca-to-verbatim";
    public static final String VERBATIM_TO_INTERPRETED = "verbatim-to-interpreted";
    public static final String INTERPRETED_TO_INDEX = "interpreted-to-index";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Conversion {

      public static final String FILE_NAME = "verbatim";

    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Interpretation {

      public static final String DIRECTORY_NAME = "interpreted";
      public static final String FILE_NAME = "interpret-";

      public enum RecordType {
        ALL,
        // Raw
        VERBATIM,
        // Core types
        METADATA,
        BASIC,
        TEMPORAL,
        LOCATION,
        TAXONOMY,
        // Extension types
        IMAGE,
        MULTIMEDIA,
        AUDUBON,
        MEASUREMENT_OR_FACT,
        AMPLIFICATION,
        // Specific
        AUSTRALIA_SPATIAL
      }

    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Indexing {

      public static final String INDEX_TYPE = "record";

    }

  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Metrics {

    public static final String AVRO_TO_JSON_COUNT = "avroToJsonCount";
    public static final String DWCA_TO_ER_COUNT = "dwcaToErCount";
    public static final String XML_TO_ER_COUNT = "xmlToErCount";

    public static final String UNIQUE_IDS_COUNT = "uniqueIdsCount";
    public static final String DUPLICATE_IDS_COUNT = "duplicatedIdsCount";
    public static final String IDENTICAL_OBJECTS_COUNT = "identicalObjectsCount";

    public static final String OCCURRENCE_EXT_COUNT = "occurrenceExtCount";
    public static final String HASH_ID_COUNT = "hashIdCount";
    // Core types
    public static final String METADATA_RECORDS_COUNT = "metadataRecordsCount";
    public static final String BASIC_RECORDS_COUNT = "basicRecordsCount";
    public static final String TEMPORAL_RECORDS_COUNT = "temporalRecordsCount";
    public static final String LOCATION_RECORDS_COUNT = "locationRecordsCount";
    public static final String TAXON_RECORDS_COUNT = "taxonRecordsCount";
    // Extension types
    public static final String MULTIMEDIA_RECORDS_COUNT = "multimediaRecordsCount";
    public static final String IMAGE_RECORDS_COUNT = "imageRecordsCount";
    public static final String AUDUBON_RECORDS_COUNT = "audubonRecordsCount";
    public static final String MEASUREMENT_OR_FACT_RECORDS_COUNT = "measurementOrFactRecordsCount";
    public static final String AMPLIFICATION_RECORDS_COUNT = "amplificationRecordsCount";
    // Specific
    public static final String AUSTRALIA_SPATIAL_RECORDS_COUNT = "australiaSpatialRecordsCount";
  }

}
