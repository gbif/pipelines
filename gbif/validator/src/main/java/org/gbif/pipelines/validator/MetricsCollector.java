package org.gbif.pipelines.validator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.CountRequestBuilder.TermCountRequest;
import org.gbif.pipelines.validator.factory.ElasticsearchClientFactory;

@Builder
public class MetricsCollector {

  private final String[] esHost;
  private final Set<Term> coreTerms;
  private final Map<Extension, Set<Term>> extensionsTerms;
  private final String datasetKey;
  private final String index;
  private final String corePrefix;
  private final String extensionsPrefix;

  public Metrics collect() {

    // Query ES - all core terms
    Map<Term, Long> coreTermCountMap = queryTermsCount(corePrefix, coreTerms);

    // Query ES  - all extensions terms
    Map<Extension, Map<Term, Long>> extensionsTermsCountMap = new HashMap<>();
    extensionsTerms.forEach(
        (key, value) -> {
          String extPrefix = extensionsPrefix + "." + key.getRowType();
          Map<Term, Long> extTermCountMap = queryTermsCount(extPrefix, value);
          extensionsTermsCountMap.put(key, extTermCountMap);
        });

    // Query ES - all OccurrenceIssues

    return Metrics.builder()
        .coreTermsCountMap(coreTermCountMap)
        .extensionsTermsCountMap(extensionsTermsCountMap)
        .build();
  }

  private Map<Term, Long> queryTermsCount(String prefix, Set<Term> terms) {
    return terms.stream()
        .parallel()
        .map(term -> buildCountRequest(prefix, term))
        .collect(Collectors.toMap(TermCountRequest::getTerm, t -> getCount(t.getCountRequest())));
  }

  private TermCountRequest buildCountRequest(String prefix, Term term) {
    return CountRequestBuilder.builder()
        .prefix(prefix)
        .datasetKey(datasetKey)
        .indexName(index)
        .build()
        .getTermCountReques(term);
  }

  @SneakyThrows
  private long getCount(CountRequest countRequest) {
    RestHighLevelClient client = ElasticsearchClientFactory.getInstance(esHost);
    return client.count(countRequest, RequestOptions.DEFAULT).getCount();
  }
}
