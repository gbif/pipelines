package org.gbif.pipelines.validator.metircs.es;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Similir to _search API call
 *
 * <p>{ "size": 0, "query": { "term": { "datasetKey": "6a54c048-dcd4-43c5-9218-03dafad7ad20" } },
 * "aggs": { "langs": { "terms": { "field": "issues", "size": 500 } } } }
 */
@Slf4j
@Builder
public class OccurrenceIssuesRequestBuilder {

  public static final String AGGREGATION = "by_issues";

  private final String datasetKey;
  private final String indexName;

  public SearchRequest getRequest() {
    return new SearchRequest()
        .source(
            new SearchSourceBuilder()
                .size(0)
                .query(QueryBuilders.termQuery("datasetKey", datasetKey))
                .aggregation(AggregationBuilders.terms(AGGREGATION).field("issues").size(1_024)))
        .indices(indexName);
  }
}
