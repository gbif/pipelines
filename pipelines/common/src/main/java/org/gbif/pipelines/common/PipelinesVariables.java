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

    public static final String ARCHIVE_TO_VERBATIM = "archive-to-verbatim";
    public static final String VERBATIM_TO_INTERPRETED = "verbatim-to-interpreted";
    public static final String INTERPRETED_TO_INDEX = "interpreted-to-index";
    public static final String INTERPRETED_TO_HDFS = "interpreted-to-hdfs";

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
        AUSTRALIA_SPATIAL,
        OCCURRENCE_HDFS_RECORD
      }

    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Indexing {

      public static final String INDEX_TYPE = "record";
      public static final String GBIF_ID = "gbifId";
    }


    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class HdfsView {

      public static final String VIEW_OCCURRENCE = "view_occurrence";
    }

  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Metrics {

    public static final String AVRO_TO_JSON_COUNT = "avroToJsonCount";
    public static final String ARCHIVE_TO_ER_COUNT = "archiveToErCount";
    public static final String AVRO_TO_HDFS_COUNT = "avroToHdfsCount";

    public static final String UNIQUE_IDS_COUNT = "uniqueIdsCount";
    public static final String DUPLICATE_IDS_COUNT = "duplicatedIdsCount";
    public static final String IDENTICAL_OBJECTS_COUNT = "identicalObjectsCount";

    public static final String OCCURRENCE_EXT_COUNT = "occurrenceExtCount";
    public static final String HASH_ID_COUNT = "hashIdCount";
    public static final String MISSED_GBIF_ID_COUNT = "missedGbifIdCount";
    // Core types
    public static final String METADATA_RECORDS_COUNT = "metadataRecordsCount";
    public static final String BASIC_RECORDS_COUNT = "basicRecordsCount";
    public static final String TEMPORAL_RECORDS_COUNT = "temporalRecordsCount";
    public static final String LOCATION_RECORDS_COUNT = "locationRecordsCount";
    public static final String TAXON_RECORDS_COUNT = "taxonRecordsCount";
    public static final String VERBATIM_RECORDS_COUNT = "verbatimRecordsCount";
    // Extension types
    public static final String MULTIMEDIA_RECORDS_COUNT = "multimediaRecordsCount";
    public static final String IMAGE_RECORDS_COUNT = "imageRecordsCount";
    public static final String AUDUBON_RECORDS_COUNT = "audubonRecordsCount";
    public static final String MEASUREMENT_OR_FACT_RECORDS_COUNT = "measurementOrFactRecordsCount";
    public static final String AMPLIFICATION_RECORDS_COUNT = "amplificationRecordsCount";

    public static final String HDFS_VIEW_RECORDS_COUNT = "hdfsViewRecordsCount";
    // Specific
    public static final String AUSTRALIA_SPATIAL_RECORDS_COUNT = "australiaSpatialRecordsCount";
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Lock {

    //Elasticsearch lock prefix
    public static final String ES_LOCK_PREFIX = "es.";

    //HDFS view lock prefix
    public static final String HDFS_LOCK_PREFIX = "hdfs.";
  }


}
