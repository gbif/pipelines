package org.gbif.pipelines.estools.service;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Utility class to store ES queries. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsQueries {

  static final String AGG_BY_INDEX = "index_agg";

  static final String FIND_DATASET_INDEXES_QUERY =
      "{\n"
          + "  \"size\": 0,"
          + "  \"aggs\": {"
          + "    \""
          + AGG_BY_INDEX
          + "\": {"
          + "      \"terms\": {\"field\": \"_index\"}"
          + "    }"
          + "  },"
          + "  \"query\": {"
          + "    \"match\": {"
          + "      \"datasetKey\": \"%s\""
          + "    }"
          + "  }"
          + "}";

  public static final String DELETE_BY_DATASET_QUERY =
      "{\"query\":{\"match\":{\"datasetKey\":\"%s\"}}}";
}
