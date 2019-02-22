package org.gbif.pipelines.common;

/**
 * Simple class with constants, general idea to have clean jar with constant only
 */
public class PipelinesVariables {

  private PipelinesVariables() {}

  public static class Pipeline {

    private Pipeline() {}

    public static final String AVRO_EXTENSION = ".avro";

    public static final String DWCA_TO_VERBATIM = "dwca-to-verbatim";
    public static final String VERBATIM_TO_INTERPRETED = "verbatim-to-interpreted";
    public static final String INTERPRETED_TO_INDEX = "interpreted-to-index";

    public static class Conversion {

      private Conversion() {}

      public static final String FILE_NAME = "verbatim";

    }

    public static class Interpretation {

      private Interpretation() {}

      public static final String DIRECTORY_NAME = "interpreted";
      public static final String FILE_NAME = "interpret-";

      public enum RecordType {
        ALL,
        METADATA,
        BASIC,
        TEMPORAL,
        MULTIMEDIA,
        IMAGE,
        LOCATION,
        TAXONOMY,
        VERBATIM
      }

    }

    public static class Indexing {

      private Indexing() {}

      public static final String INDEX_TYPE = "record";

    }

  }

  public static class Metrics {

    private Metrics() {}

    public static final String AVRO_TO_JSON_COUNT = "avroToJsonCount";
    public static final String DWCA_TO_AVRO_COUNT = "dwcaToAvroCount";
    public static final String MULTIMEDIA_RECORDS_COUNT = "multimediaRecordsCount";
    public static final String IMAGE_RECORDS_COUNT = "imageRecordsCount";
    public static final String TEMPORAL_RECORDS_COUNT = "temporalRecordsCount";
    public static final String BASIC_RECORDS_COUNT = "basicRecordsCount";
    public static final String LOCATION_RECORDS_COUNT = "locationRecordsCount";
    public static final String METADATA_RECORDS_COUNT = "metadataRecordsCount";
    public static final String TAXON_RECORDS_COUNT = "taxonRecordsCount";
    public static final String UNIQUE_IDS_COUNT = "uniqueIdsCount";
    public static final String DUPLICATE_IDS_COUNT = "duplicatedIdsCount";

  }

}
