package org.gbif.pipelines.spark;

/** Holder class for directory names used in Spark pipelines. */
public interface Directories {

  /** Directory for event HDFS parquet files ready to be loaded into Hive */
  String EVENT_HDFS = "event_hdfs";
  /** Directory for event inherited fields parquet files ready to be loaded into Hive */
  String EVENT_INHERITED_FIELDS = "event_inherited_fields";
  /** Directory for event parquet files ready to be loaded into Elastic */
  String EVENT_JSON = "event_json";
  /** Directory of identifier */
  String EXTENDED_IDENTIFIERS = "extended_identifiers";

  /**
   * Final processed identifiers directory. All the identifiers in this directory are valid and
   * persisted to hbase
   */
  String IDENTIFIERS = "identifiers";

  /** Directory for identifiers that are not currently persisted to hbase. */
  String IDENTIFIERS_ABSENT = "identifiers_absent";

  String IDENTIFIERS_TRANSFORMED = "identifiers_transformed";
  /** Directory for identifiers that have been validates */
  String IDENTIFIERS_VALID = "identifiers_valid";

  String IDENTIFIERS_INVALID = "identifiers_invalid";
  /** Directory for occurrence parquet files ready to be loaded into Hive */
  String OCCURRENCE_HDFS = "hdfs";
  /** Directory for occurrence parquet files ready to be loaded into Elastic */
  String OCCURRENCE_JSON = "json";
  /**
   * Directory for verbatim occurrence parquet files used as input for interpretation. These have
   * been loaded from the original verbatim AVRO, default values applied
   */
  String OCCURRENCE_VERBATIM = "verbatim";
  /** Directory for simple event parquet files generated as output from interpretation */
  String SIMPLE_EVENT = "simple_event";

  String SIMPLE_EVENT_WITH_DERIVED = "simple_event_with_derived";
  /** Directory for simple occurrence parquet files used as input for interpretation */
  String SIMPLE_OCCURRENCE = "simple_occurrence";
  /** Directory for extended verbatim files after filtering out records with missing core ID */
  String VERBATIM_EXT_FILTERED = "verbatim_ext_filtered";
}
