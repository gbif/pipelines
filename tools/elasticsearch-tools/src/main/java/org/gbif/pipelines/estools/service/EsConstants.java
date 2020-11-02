package org.gbif.pipelines.estools.service;

import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Utility class to store ES-related constants. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EsConstants {

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static final class Field {

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
    public static final String ANALYSIS = "analysis";
    public static final String MAX_RESULT_WINDOW = "max_result_window";

    public static final String INDEX_REFRESH_INTERVAL = Util.INDEX_PREFIX + Field.REFRESH_INTERVAL;
    public static final String INDEX_NUMBER_SHARDS = Util.INDEX_PREFIX + Field.NUMBER_SHARDS;
    public static final String INDEX_NUMBER_REPLICAS = Util.INDEX_PREFIX + Field.NUMBER_REPLICAS;
    public static final String INDEX_TRANSLOG_DURABILITY =
        Util.INDEX_PREFIX + Field.TRANSLOG + Util.JSON_CONCATENATOR + Field.DURABILITY;
    public static final String INDEX_ANALYSIS = Util.INDEX_PREFIX + Field.ANALYSIS;
    public static final String INDEX_MAX_RESULT_WINDOW = Util.INDEX_PREFIX + MAX_RESULT_WINDOW;
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static final class Action {

    public static final String ADD = "add";
    public static final String REMOVE_INDEX = "remove_index";
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static final class Util {

    public static final String INDEX_SEPARATOR = "_";
    private static final String JSON_CONCATENATOR = ".";
    private static final String INDEX_PREFIX = Field.INDEX + JSON_CONCATENATOR;
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static final class Indexing {

    public static final String REFRESH_INTERVAL = "-1";
    public static final String NUMBER_REPLICAS = "0";
    public static final String ANALYSIS =
        "{\"normalizer\":{\"lowercase_normalizer\":{\"type\":\"custom\",\"char_filter\":[],\"filter\":[\"lowercase\"]}},"
            + "\"analyzer\": {\"lowercase_analyzer\": {\"filter\": [\"lowercase\"],\"tokenizer\": \"keyword\",\"type\": \"custom\",\"char_filter\": []}}}";
    private static final Map<String, String> DEFAULT_INDEXING_SETTINGS = new HashMap<>();

    static {
      DEFAULT_INDEXING_SETTINGS.put(Field.INDEX_REFRESH_INTERVAL, Indexing.REFRESH_INTERVAL);
      DEFAULT_INDEXING_SETTINGS.put(Field.INDEX_NUMBER_SHARDS, Constant.NUMBER_SHARDS);
      DEFAULT_INDEXING_SETTINGS.put(Field.INDEX_NUMBER_REPLICAS, Indexing.NUMBER_REPLICAS);
      DEFAULT_INDEXING_SETTINGS.put(Field.INDEX_TRANSLOG_DURABILITY, Constant.TRANSLOG_DURABILITY);
      DEFAULT_INDEXING_SETTINGS.put(Field.INDEX_ANALYSIS, Indexing.ANALYSIS);
    }

    public static Map<String, String> getDefaultIndexingSettings() {
      return DEFAULT_INDEXING_SETTINGS;
    }
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static final class Searching {

    public static final String REFRESH_INTERVAL = "1s";
    public static final String NUMBER_REPLICAS = "1";
    private static final Map<String, String> DEFAULT_SEARCH_SETTINGS = new HashMap<>();

    static {
      DEFAULT_SEARCH_SETTINGS.put(Field.INDEX_REFRESH_INTERVAL, Searching.REFRESH_INTERVAL);
      DEFAULT_SEARCH_SETTINGS.put(Field.INDEX_NUMBER_REPLICAS, Searching.NUMBER_REPLICAS);
    }

    public static Map<String, String> getDefaultSearchSettings() {
      return DEFAULT_SEARCH_SETTINGS;
    }
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static final class Constant {

    public static final String NUMBER_SHARDS = "3";
    public static final String TRANSLOG_DURABILITY = "async";
  }
}
