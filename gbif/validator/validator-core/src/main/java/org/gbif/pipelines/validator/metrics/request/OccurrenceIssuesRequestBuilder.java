package org.gbif.pipelines.validator.metrics.request;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;

/**
 * Similir to _search API call
 *
 * <p>{"size":0,"query":{"term":{"datasetKey":"d596fccb-2319-42eb-b13b-986c932780ad"}},
 * "aggs":{"issues":{"terms":{"field":"issues","size":500},
 * "aggs":{"hits":{"top_hits":{"_source":{"includes":["verbatim"]},"size":5}}}}}}
 */
@Slf4j
@Builder
public class OccurrenceIssuesRequestBuilder {

  public static final String ISSUES_AGGREGATION = "by_issues";
  public static final String HITS_AGGREGATION = "by_hits";

  private final String termValue;
  private final String indexName;
  @Builder.Default private final int size = 0;
  @Builder.Default private final int subSize = 10;
  @Builder.Default private final String termName = Indexing.DATASET_KEY;
  @Builder.Default private final String aggsField = Indexing.ISSUES;
  @Builder.Default private final String[] includeFields = {Indexing.ID, Indexing.VERBATIM};
  @Builder.Default private final String[] excludeFields = null;

  public SearchRequest getRequest() {

    TermQueryBuilder filterByDatasetKey = QueryBuilders.termQuery(termName, termValue);

    TopHitsAggregationBuilder aggregateHits =
        AggregationBuilders.topHits(HITS_AGGREGATION)
            .size(subSize)
            .fetchSource(includeFields, null);

    TermsAggregationBuilder aggregateIssues =
        AggregationBuilders.terms(ISSUES_AGGREGATION)
            .field(aggsField)
            .size(1_024)
            .subAggregation(aggregateHits);

    return new SearchRequest()
        .source(
            new SearchSourceBuilder()
                .size(size)
                .query(filterByDatasetKey)
                .aggregation(aggregateIssues))
        .indices(indexName);
  }
}
