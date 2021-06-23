package org.gbif.pipelines.validator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.gbif.dwc.terms.Term;

@Builder
public class CountRequestBuilder {

  private final String datasetKey;
  private final String prefix;
  private final String indexName;

  public TermCountRequest getTermCountReques(Term term) {
    CountRequest countRequest = new CountRequest();

    BoolQueryBuilder boolQueryBuilder =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("datasetKey", datasetKey))
            .must(QueryBuilders.existsQuery(prefix + "." + term.qualifiedName()));

    CountRequest request = countRequest.query(boolQueryBuilder).indices(indexName);

    return TermCountRequest.create(term, request);
  }

  @Getter
  @AllArgsConstructor(staticName = "create")
  public static class TermCountRequest {
    private final Term term;
    private final CountRequest countRequest;
  }
}
