package org.gbif.pipelines.validator.metircs.es;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.gbif.dwc.terms.Term;

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

  private final String datasetKey;
  private final String prefix;
  private final String indexName;

  public TermCountRequest getRequest(Term term) {

    CountRequest request =
        new CountRequest()
            .query(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("datasetKey", datasetKey))
                    .must(QueryBuilders.existsQuery(prefix + "." + term.qualifiedName())))
            .indices(indexName);

    return TermCountRequest.create(term, request);
  }

  @Getter
  @AllArgsConstructor(staticName = "create")
  public static class TermCountRequest {
    private final Term term;
    private final CountRequest countRequest;
  }
}
