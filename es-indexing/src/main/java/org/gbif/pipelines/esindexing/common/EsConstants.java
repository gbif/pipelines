package org.gbif.pipelines.esindexing.common;

/**
 * Utility class to store ES-related constants.
 */
public final class EsConstants {

  // Fields
  public static final String INDEX_FIELD = "index";
  public static final String SETTINGS_FIELD = "settings";
  public static final String REFRESH_INTERVAL_FIELD = "refresh_interval";
  public static final String NUMBER_SHARDS_FIELD = "number_of_shards";
  public static final String NUMBER_REPLICAS_FIELD = "number_of_replicas";
  public static final String TRANSLOG_FIELD = "translog";
  public static final String DURABILITY_FIELD = "durability";
  public static final String ACTIONS_FIELD = "actions";
  public static final String MAPPINGS_FIELD = "mappings";
  public static final String ALIAS_FIELD = "alias";

  // actions
  public static final String ADD_ACTION = "add";
  public static final String REMOVE_INDEX_ACTION = "remove_index";

  // utils
  private static final String JSON_CONCATENATOR = ".";
  private static final String INDEX_PREFIX = INDEX_FIELD + JSON_CONCATENATOR;

  // Index fields concatenated to the index prefix
  public static final String INDEX_REFRESH_INTERVAL_FIELD = INDEX_PREFIX + REFRESH_INTERVAL_FIELD;
  public static final String INDEX_NUMBER_SHARDS_FIELD = INDEX_PREFIX + NUMBER_SHARDS_FIELD;
  public static final String INDEX_NUMBER_REPLICAS_FIELD = INDEX_PREFIX + NUMBER_REPLICAS_FIELD;
  public static final String INDEX_DURABILITY_FIELD =
    INDEX_PREFIX + TRANSLOG_FIELD + JSON_CONCATENATOR + DURABILITY_FIELD;

  // indexing values
  public static final String INDEXING_REFRESH_INTERVAL = "-1";
  public static final String INDEXING_NUMBER_REPLICAS = "0";

  // searching values
  public static final String SEARCHING_REFRESH_INTERVAL = "1s";
  public static final String SEARCHING_NUMBER_REPLICAS = "1";

  // constant values
  public static final String NUMBER_SHARDS = "3";
  public static final String TRANSLOG_DURABILITY = "async";

  private EsConstants() {}

}
