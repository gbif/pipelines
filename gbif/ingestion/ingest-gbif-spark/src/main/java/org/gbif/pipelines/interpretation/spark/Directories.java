package org.gbif.pipelines.interpretation.spark;

/** Holder class for directory names used in Spark pipelines. */
public interface Directories {

  /** Directory for event HDFS parquet files ready to be loaded into Hive */
  String EVENT_HDFS = "event-hdfs";
  /** Directory for event inherited fields parquet files ready to be loaded into Hive */
  String EVENT_INHERITED_FIELDS = "event-inherited-fields";
  /** Directory for event parquet files ready to be loaded into Elastic */
  String EVENT_JSON = "event-json";
  /** Directory of identifier */
  String EXTENDED_IDENTIFIERS = "extended-identifiers";

  /**
   * Final processed identifiers directory. All the identifiers in this directory are valid and
   * persisted to hbase
   */
  String IDENTIFIERS = "identifiers";

  /** Directory for identifiers that are not currently persisted to hbase. */
  String IDENTIFIERS_ABSENT = "identifiers_absent";

  String IDENTIFIERS_TRANSFORMED = "identifiers_transformed";
  String IDENTIFIERS_VALID = "identifiers_valid";
  /** Directory for occurrence parquet files ready to be loaded into Hive */
  String OCCURRENCE_HDFS = "hdfs";
  /** Directory for occurrence parquet files ready to be loaded into Elastic */
  String OCCURRENCE_JSON = "json";
  /**
   * Directory for verbatim occurrence parquet files used as input for interpretation. These have
   * been loaded from the original verbatim AVRO, default values applied
   */
  String OCCURRENCE_VERBATIM = "verbatim";

  String SIMPLE_EVENT = "simple-event";
  String SIMPLE_EVENT_WITH_INHERITED = "simple-event-with-inherited";
  /** Directory for simple occurrence parquet files used as input for interpretation */
  String SIMPLE_OCCURRENCE = "simple-occurrence";

  String VERBATIM_EXT_FILTERED = "verbatim_ext_filtered";
}
