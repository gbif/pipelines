package org.gbif.pipelines.estools.service;

/** Utility class to store ES-related constants. */
final class EsConstants {

  private EsConstants() {}

  static final class Field {
    private Field() {}

    static final String INDEX = "index";
    static final String SETTINGS = "settings";
    static final String REFRESH_INTERVAL = "refresh_interval";
    static final String NUMBER_SHARDS = "number_of_shards";
    static final String NUMBER_REPLICAS = "number_of_replicas";
    static final String TRANSLOG = "translog";
    static final String DURABILITY = "durability";
    static final String ACTIONS = "actions";
    static final String MAPPINGS = "mappings";
    static final String ALIAS = "alias";
    static final String COUNT = "count";

    static final String INDEX_REFRESH_INTERVAL = Util.INDEX_PREFIX + Field.REFRESH_INTERVAL;
    static final String INDEX_NUMBER_SHARDS = Util.INDEX_PREFIX + Field.NUMBER_SHARDS;
    static final String INDEX_NUMBER_REPLICAS = Util.INDEX_PREFIX + Field.NUMBER_REPLICAS;
    static final String INDEX_TRANSLOG_DURABILITY =
        Util.INDEX_PREFIX + Field.TRANSLOG + Util.JSON_CONCATENATOR + Field.DURABILITY;
  }

  static final class Action {
    private Action() {}

    static final String ADD = "add";
    static final String REMOVE_INDEX = "remove_index";
  }

  private static final class Util {
    private Util() {}

    private static final String JSON_CONCATENATOR = ".";
    private static final String INDEX_PREFIX = Field.INDEX + JSON_CONCATENATOR;
  }

  static final class Indexing {
    private Indexing() {}

    static final String REFRESH_INTERVAL = "-1";
    static final String NUMBER_REPLICAS = "0";
  }

  static final class Searching {
    private Searching() {}

    static final String REFRESH_INTERVAL = "1s";
    static final String NUMBER_REPLICAS = "1";
  }

  static final class Constant {
    private Constant() {}

    static final String NUMBER_SHARDS = "3";
    static final String TRANSLOG_DURABILITY = "async";
  }
}
