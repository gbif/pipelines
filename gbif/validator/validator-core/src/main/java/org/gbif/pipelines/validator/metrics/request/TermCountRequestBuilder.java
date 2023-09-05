package org.gbif.pipelines.validator.metrics.request;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.gbif.pipelines.validator.metrics.RawToInterpreted;

/**
 * Similir to _count API call
 *
 * <p>{ "query": { "bool": { "must": [ { "term": { "datasetKey": { "value":
 * "675a1bfd-9bcc-46ea-a417-1f68f23a10f6" } } }, { "exists": { "field":
 * "verbatim.core.http://rs.tdwg.org/dwc/terms/country" } } ] } } }
 */
@Slf4j
@Builder
public class TermCountRequestBuilder {

  @Builder.Default private final String termName = "datasetKey";
  private final String termValue;
  private final String prefix;
  private final String indexName;
  private final String term;

  public TermCountRequest getRequest() {
    CountRequest rawRequest = getRawCountRequest();
    CountRequest interpretedRequest = getinterpretedCountRequest().orElse(null);

    return TermCountRequest.create(term, rawRequest, interpretedRequest);
  }

  private Optional<CountRequest> getinterpretedCountRequest() {
    if (term == null) {
      return Optional.empty();
    }

    return RawToInterpreted.getInterpretedField(term)
        .map(
            field ->
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(termName, this.termValue))
                    .must(QueryBuilders.existsQuery(field)))
        .map(bqb -> new CountRequest().query(bqb).indices(indexName));
  }

  private CountRequest getRawCountRequest() {
    BoolQueryBuilder boolQueryBuilder =
        QueryBuilders.boolQuery().must(QueryBuilders.termQuery(termName, this.termValue));

    if (term != null) {
      String exists = prefix == null || prefix.isEmpty() ? term : prefix + "." + term;

      boolQueryBuilder = boolQueryBuilder.must(QueryBuilders.existsQuery(exists));
    }

    return new CountRequest().query(boolQueryBuilder).indices(indexName);
  }

  @AllArgsConstructor(staticName = "create")
  public static class TermCountRequest {

    @Getter private final String term;
    @Getter private final CountRequest rawCountRequest;
    private final CountRequest interpretedCountRequest;

    public Optional<CountRequest> getInterpretedCountRequest() {
      return Optional.ofNullable(interpretedCountRequest);
    }
  }
}
