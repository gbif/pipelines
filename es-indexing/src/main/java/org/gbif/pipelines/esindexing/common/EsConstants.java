package org.gbif.pipelines.esindexing.common;

public class EsConstants {

  public static final String JSON_CONCATENATOR = ".";

  // Fields
  public static final String INDEX_FIELD = "index";
  public static final String SETTINGS_FIELD = "settings";
  public static final String REFRESH_INTERVAL_FIELD = "refresh_interval";
  public static final String NUMBER_SHARDS_FIELD = "number_of_shards";
  public static final String NUMBER_REPLICAS_FIELD = "number_of_replicas";
  public static final String TRANSLOG_FIELD = "translog";
  public static final String DURABILITY_FIELD = "durability";

  // indexing values
  public static final String INDEXING_REFRESH_INTERVAL = "-1";
  public static final String INDEXING_NUMBER_REPLICAS = "0";

  // searching values
  public static final String SEARCHING_REFRESH_INTERVAL = "1s";
  public static final String SEARCHING_NUMBER_REPLICAS = "1";

  // constant values
  public static final String NUMBER_SHARDS = "3";
  public static final String TRANSLOG_DURABILITY = "async";



}
