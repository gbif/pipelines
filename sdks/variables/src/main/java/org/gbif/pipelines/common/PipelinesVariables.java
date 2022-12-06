package org.gbif.pipelines.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Simple class with constants, general idea to have clean jar with constant only */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesVariables {

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Pipeline {

    public static final String AVRO_EXTENSION = ".avro";
    public static final String ALL_AVRO = "*" + AVRO_EXTENSION;

    public static final String ARCHIVE_TO_VERBATIM = "archive-to-verbatim";
    public static final String CAMTRAPDP_TO_DWCA = "camtrapdp-to-dwca";
    public static final String VERBATIM_TO_OCCURRENCE = "verbatim-to-occurrence";
    public static final String VERBATIM_TO_IDENTIFIER = "verbatim-to-identifier";
    public static final String OCCURRENCE_TO_INDEX = "occurrence-to-index";
    public static final String OCCURRENCE_TO_HDFS = "occurrence-to-hdfs";
    public static final String FRAGMENTER = "fragmenter";
    public static final String VALIDATOR = "validator";
    public static final String COLLECT_METRICS = "collect-metrics";
    public static final String VERBATIM_TO_EVENT = "verbatim-to-event";
    public static final String EVENT_TO_INDEX = "event-to-index";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Conversion {

      public static final String FILE_NAME = "verbatim";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Interpretation {

      public static final String FILE_NAME = "interpret-";
      public static final String CRAP_USER = "crap";
      public static final String USER_GROUP = "supergroup";

      public interface InterpretationType extends Serializable {

        String name();

        String all();
      }

      public enum RecordType implements InterpretationType {
        ALL,
        // Raw
        VERBATIM,
        // Core types
        IDENTIFIER,
        EVENT_IDENTIFIER,
        IDENTIFIER_ABSENT,
        METADATA,
        BASIC,
        CLUSTERING,
        TEMPORAL,
        LOCATION,
        TAXONOMY,
        GRSCICOLL,
        EVENT,
        // Extension types
        IMAGE,
        MULTIMEDIA,
        AUDUBON,
        MEASUREMENT_OR_FACT,
        AMPLIFICATION,
        // Specific
        LOCATION_FEATURE,
        // Tables,
        // Remeber to add mapping to org.gbif.pipelines.core.utils.DwcaExtensionTermUtils
        // and org.gbif.pipelines.ingest.utils.HdfsViewAvroUtils
        OCCURRENCE,
        MEASUREMENT_OR_FACT_TABLE,
        IDENTIFICATION_TABLE,
        RESOURCE_RELATIONSHIP_TABLE,
        AMPLIFICATION_TABLE,
        CLONING_TABLE,
        GEL_IMAGE_TABLE,
        LOAN_TABLE,
        MATERIAL_SAMPLE_TABLE,
        PERMIT_TABLE,
        PREPARATION_TABLE,
        PRESERVATION_TABLE,
        GERMPLASM_MEASUREMENT_SCORE_TABLE,
        GERMPLASM_MEASUREMENT_TRAIT_TABLE,
        GERMPLASM_MEASUREMENT_TRIAL_TABLE,
        GERMPLASM_ACCESSION_TABLE,
        EXTENDED_MEASUREMENT_OR_FACT_TABLE,
        CHRONOMETRIC_AGE_TABLE,
        REFERENCE_TABLE,
        IDENTIFIER_TABLE,
        AUDUBON_TABLE,
        IMAGE_TABLE,
        MULTIMEDIA_TABLE,
        DNA_DERIVED_DATA_TABLE;

        @Override
        public String all() {
          return ALL.name();
        }

        public static Set<RecordType> getAllInterpretation() {
          return new HashSet<>(
              Arrays.asList(
                  VERBATIM,
                  // Core types
                  METADATA,
                  IDENTIFIER_ABSENT,
                  CLUSTERING,
                  BASIC,
                  TEMPORAL,
                  LOCATION,
                  TAXONOMY,
                  GRSCICOLL,
                  // Extension types
                  IMAGE,
                  MULTIMEDIA,
                  AUDUBON,
                  MEASUREMENT_OR_FACT,
                  AMPLIFICATION,
                  // Specific
                  LOCATION_FEATURE,
                  OCCURRENCE));
        }

        public static Set<String> getAllInterpretationAsString() {
          return getAllInterpretation().stream().map(RecordType::name).collect(Collectors.toSet());
        }

        public static Set<String> getAllValidatorInterpretationAsString() {
          Set<String> set = getAllInterpretationAsString();
          set.add(IDENTIFIER.name());
          set.remove(IDENTIFIER_ABSENT.name());
          return set;
        }

        public static Set<RecordType> getAllTables() {
          return new HashSet<>(
              Arrays.asList(
                  OCCURRENCE,
                  MEASUREMENT_OR_FACT_TABLE,
                  IDENTIFICATION_TABLE,
                  RESOURCE_RELATIONSHIP_TABLE,
                  AMPLIFICATION_TABLE,
                  CLONING_TABLE,
                  GEL_IMAGE_TABLE,
                  LOAN_TABLE,
                  MATERIAL_SAMPLE_TABLE,
                  PERMIT_TABLE,
                  PREPARATION_TABLE,
                  PRESERVATION_TABLE,
                  GERMPLASM_MEASUREMENT_SCORE_TABLE,
                  GERMPLASM_MEASUREMENT_TRAIT_TABLE,
                  GERMPLASM_MEASUREMENT_TRIAL_TABLE,
                  GERMPLASM_ACCESSION_TABLE,
                  EXTENDED_MEASUREMENT_OR_FACT_TABLE,
                  CHRONOMETRIC_AGE_TABLE,
                  REFERENCE_TABLE,
                  IDENTIFIER_TABLE,
                  AUDUBON_TABLE,
                  IMAGE_TABLE,
                  MULTIMEDIA_TABLE,
                  DNA_DERIVED_DATA_TABLE));
        }
      }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Indexing {

      public static final String INDEX_TYPE = "record";

      // Fields
      public static final String ID = "id";
      public static final String GBIF_ID = "gbifId";
      public static final String VERBATIM = "verbatim";
      public static final String CORE = "core";
      public static final String EXTENSIONS = "extensions";
      public static final String ISSUES = "issues";
      public static final String NOT_ISSUES = "notIssues";
      public static final String DATASET_KEY = "datasetKey";
      public static final String CREATED = "created";
      public static final String DECIMAL_LATITUDE = "decimalLatitude";
      public static final String DECIMAL_LONGITUDE = "decimalLongitude";
      public static final String MACHINE_TAGS = "machineTags";
      public static final String CRAWL_ID = "crawlId";
      public static final String LICENSE = "license";
      public static final String ALL = "all";
      public static final String DATASET_PUBLISHING_COUNTRY = "datasetPublishingCountry";
      public static final String MEASUREMENT_OR_FACT_ITEMS = "measurementOrFactItems";
      public static final String GBIF_CLASSIFICATION = "gbifClassification";
      public static final String YEAR = "year";
      public static final String DAY = "day";
      public static final String MONTH = "month";
      public static final String EVENT_DATE_SINGLE = "eventDateSingle";
      public static final String EVENT_DATE = "eventDate";
      public static final String GTE = "gte";
      public static final String LTE = "lte";
      public static final String LOCATION_FEATUE_LAYERS = "locationFeatureLayers";
      public static final String COUNTRY = "country";
      public static final String COUNTRY_CODE = "countryCode";
      public static final String COORDINATES = "coordinates";
      public static final String SCOORDINATES = "scoordinates";
      public static final String LOCALITY = "locality";
      public static final String IS_CLUSTERED = "isClustered";
      public static final String RECORDED_BY = "recordedBy";
      public static final String RECORDED_BY_JOINED = "recordedByJoined";
      public static final String START_DAY_OF_YEAR = "startDayOfYear";
      public static final String IDENTIFIED_BY = "identifiedBy";
      public static final String IDENTIFIED_BY_JOINED = "identifiedByJoined";
      public static final String HOSTING_ORGANIZATION_KEY = "hostingOrganizationKey";
      public static final String DATASET_ID = "datasetID";
      public static final String DATASET_NAME = "datasetName";
      public static final String OTHER_CATALOG_NUMBERS = "otherCatalogNumbers";
      public static final String OTHER_CATALOG_NUMBERS_JOINED = "otherCatalogNumbersJoined";
      public static final String PREPARATIONS = "preparations";
      public static final String PREPARATIONS_JOINED = "preparationsJoined";
      public static final String SAMPLING_PROTOCOL = "samplingProtocol";
      public static final String SAMPLING_PROTOCOL_JOINED = "samplingProtocolJoined";
      public static final String TYPE_STATUS = "typeStatus";
      public static final String PROJECT_ID = "projectId";
      public static final String PROJECT_ID_JOINED = "projectIdJoined";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Identifier {
      public static final String GBIF_ID_EMPTY = "GBIF_ID_EMPTY";
      public static final String GBIF_ID_INVALID = "GBIF_ID_INVALID";
      public static final String GBIF_ID_ABSENT = "GBIF_ID_ABSENT";
    }
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Metrics {

    public static final String AVRO_TO_JSON_COUNT = "avroToJsonCount";

    public static final String OCC_AVRO_TO_JSON_COUNT = "occAvroToJsonCount";
    public static final String EVENTS_AVRO_TO_JSON_COUNT = "eventsAvroToJsonCount";
    public static final String ARCHIVE_TO_ER_COUNT = "archiveToErCount";
    public static final String AVRO_TO_HDFS_COUNT = "avroToHdfsCount";
    // GBIF ID
    public static final String FILTERED_GBIF_IDS_COUNT = "filteredGbifIdsCount";
    public static final String UNIQUE_GBIF_IDS_COUNT = "uniqueGbifIdsCount";
    public static final String DUPLICATE_GBIF_IDS_COUNT = "duplicatedGbifIdsCount";
    public static final String IDENTICAL_GBIF_OBJECTS_COUNT = "identicalGbifObjectsCount";
    public static final String INVALID_GBIF_ID_COUNT = "invalidGbifIdCount";
    public static final String ABSENT_GBIF_ID_COUNT = "absentGbifIdCount";
    // Occurrence
    public static final String UNIQUE_IDS_COUNT = "uniqueIdsCount";
    public static final String DUPLICATE_IDS_COUNT = "duplicatedIdsCount";
    public static final String IDENTICAL_OBJECTS_COUNT = "identicalObjectsCount";
    public static final String FILTER_ER_BASED_ON_GBIF_ID = "filterErBasedOnGbifIdCount";
    public static final String OCCURRENCE_EXT_COUNT = "occurrenceExtCount";
    public static final String HASH_ID_COUNT = "hashIdCount";
    // Core types
    public static final String METADATA_RECORDS_COUNT = "metadataRecordsCount";
    public static final String DEFAULT_VALUES_RECORDS_COUNT = "defaultValuesRecordsCount";
    public static final String BASIC_RECORDS_COUNT = "basicRecordsCount";
    public static final String GBIF_ID_RECORDS_COUNT = "gbifIdRecordsCount";
    public static final String CLUSTERING_RECORDS_COUNT = "clusteringRecordsCount";
    public static final String TEMPORAL_RECORDS_COUNT = "temporalRecordsCount";
    public static final String LOCATION_RECORDS_COUNT = "locationRecordsCount";
    public static final String TAXON_RECORDS_COUNT = "taxonRecordsCount";
    public static final String GRSCICOLL_RECORDS_COUNT = "grscicollRecordsCount";
    public static final String VERBATIM_RECORDS_COUNT = "verbatimRecordsCount";
    // Event core types
    public static final String EVENT_CORE_RECORDS_COUNT = "eventCoreRecordsCount";
    // Extension types
    public static final String MULTIMEDIA_RECORDS_COUNT = "multimediaRecordsCount";
    public static final String IMAGE_RECORDS_COUNT = "imageRecordsCount";
    public static final String AUDUBON_RECORDS_COUNT = "audubonRecordsCount";
    public static final String MEASUREMENT_OR_FACT_RECORDS_COUNT = "measurementOrFactRecordsCount";
    public static final String AMPLIFICATION_RECORDS_COUNT = "amplificationRecordsCount";
    // HDFS Tables
    public static final String MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT =
        "measurementOrFactTableRecordsCount";
    public static final String IDENTIFICATION_TABLE_RECORDS_COUNT =
        "identificationTableRecordsCount";
    public static final String RESOURCE_RELATIONSHIP_TABLE_RECORDS_COUNT =
        "resourceRelationTableRecordsCount";
    public static final String AMPLIFICATION_TABLE_RECORDS_COUNT = "amplificationTableRecordsCount";
    public static final String CLONING_TABLE_RECORDS_COUNT = "cloningTableRecordsCount";
    public static final String GEL_IMAGE_TABLE_RECORDS_COUNT = "gelImageTableRecordsCount";
    public static final String LOAN_TABLE_RECORDS_COUNT = "loanTableRecordsCount";
    public static final String MATERIAL_SAMPLE_TABLE_RECORDS_COUNT =
        "materialSampleTableRecordsCount";
    public static final String PERMIT_TABLE_RECORDS_COUNT = "permitTableRecordsCount";
    public static final String PREPARATION_TABLE_RECORDS_COUNT = "preparationTableRecordsCount";
    public static final String PRESERVATION_TABLE_RECORDS_COUNT = "preservationTableRecordsCount";
    public static final String MEASUREMENT_SCORE_TABLE_RECORDS_COUNT =
        "measurementScoreTableRecordsCount";
    public static final String MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT =
        "measurementTraitTableRecordsCount";
    public static final String MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT =
        "measurementTrialTableRecordsCount";
    public static final String GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT =
        "germplasmAccessionTableRecordsCount";
    public static final String EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT =
        "extendedMeasurementOrFactTableRecordsCount";
    public static final String CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT =
        "chronometricAgeTableRecordsCount";
    public static final String REFERENCE_TABLE_RECORDS_COUNT = "referencesTableRecordsCount";
    public static final String IDENTIFIER_TABLE_RECORDS_COUNT = "identifierTableRecordsCount";
    public static final String AUDUBON_TABLE_RECORDS_COUNT = "audubonTableRecordsCount";
    public static final String IMAGE_TABLE_RECORDS_COUNT = "imageTableRecordsCount";
    public static final String MULTIMEDIA_TABLE_RECORDS_COUNT = "multimediaTableRecordsCount";
    public static final String DNA_DERIVED_DATA_TABLE_RECORDS_COUNT =
        "dnaDerivedDataTableRecordsCount";

    // Fragmenter
    public static final String FRAGMENTER_COUNT = "fragmenterRecordsCount";
    // Specific
    public static final String IDENTIFIER_RECORDS_COUNT = "identifierRecordsCount";
    public static final String LOCATION_FEATURE_RECORDS_COUNT = "locationFeatureRecordsCount";

    public static final String ATTEMPTED = "Attempted";
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class DynamicProperties {

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Parser {
      public static final String MASS = "mass";
      public static final String LENGTH = "length";
    }
  }
}
