package org.gbif.pipelines.estools.service;

/** Utility class to store ES-related constants. */
public final class EsConstants {

  private EsConstants() {}

  public static final class Field {
    private Field() {}

    public static final String INDEX = "index";
    public static final String SETTINGS = "settings";
    public static final String REFRESH_INTERVAL = "refresh_interval";
    public static final String NUMBER_SHARDS = "number_of_shards";
    public static final String NUMBER_REPLICAS = "number_of_replicas";
    public static final String TRANSLOG = "translog";
    public static final String DURABILITY = "durability";
    public static final String ACTIONS = "actions";
    public static final String MAPPINGS = "mappings";
    public static final String ALIAS = "alias";
    public static final String COUNT = "count";

    public static final String INDEX_REFRESH_INTERVAL = Util.INDEX_PREFIX + Field.REFRESH_INTERVAL;
    public static final String INDEX_NUMBER_SHARDS = Util.INDEX_PREFIX + Field.NUMBER_SHARDS;
    public static final String INDEX_NUMBER_REPLICAS = Util.INDEX_PREFIX + Field.NUMBER_REPLICAS;
    public static final String INDEX_TRANSLOG_DURABILITY =
        Util.INDEX_PREFIX + Field.TRANSLOG + Util.JSON_CONCATENATOR + Field.DURABILITY;
  }

  public static final class Action {
    private Action() {}

    public static final String ADD = "add";
    public static final String REMOVE_INDEX = "remove_index";
  }

  private static final class Util {
    private Util() {}

    private static final String JSON_CONCATENATOR = ".";
    private static final String INDEX_PREFIX = Field.INDEX + JSON_CONCATENATOR;
  }

  public static final class Indexing {
    private Indexing() {}

    public static final String REFRESH_INTERVAL = "-1";
    public static final String NUMBER_REPLICAS = "0";
  }

  public static final class Searching {
    private Searching() {}

    public static final String REFRESH_INTERVAL = "1s";
    public static final String NUMBER_REPLICAS = "1";
  }

  public static final class Constant {
    private Constant() {}

    public static final String NUMBER_SHARDS = "3";
    public static final String TRANSLOG_DURABILITY = "async";
  }
}
