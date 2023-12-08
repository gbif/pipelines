package org.gbif.pipelines.validator.metrics.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Similir to _search API call
 *
 * <p>{ "size": 0, "query": { "term": { "datasetKey": "675a1bfd-9bcc-46ea-a417-1f68f23a10f6" } },
 * "aggs": { "types_count": { "value_count": { "field":
 * "verbatim.extensions.http://rs.tdwg.org/dwc/terms/MeasurementOrFact.http://rs.tdwg.org/dwc/terms/measurementType"
 * } } } }
 */
@Slf4j
@Builder
public class ExtensionTermCountRequestBuilder {

  public static final String AGGREGATION = "types_count";

  @Builder.Default private final String termName = "datasetKey";
  private final String termValue;
  private final String prefix;
  private final String indexName;
  private final String term;

  public ExtTermCountRequest getRequest() {

    String aggsField = prefix == null || prefix.isEmpty() ? term : prefix + "." + term;

    SearchRequest request =
        new SearchRequest()
            .source(
                new SearchSourceBuilder()
                    .size(0)
                    .query(QueryBuilders.termQuery(termName, termValue))
                    .aggregation(AggregationBuilders.count(AGGREGATION).field(aggsField)))
            .indices(indexName);

    return ExtTermCountRequest.create(term, request);
  }

  @Getter
  @AllArgsConstructor(staticName = "create")
  public static class ExtTermCountRequest {
    private final String term;
    private final SearchRequest searchRequest;
  }
}
