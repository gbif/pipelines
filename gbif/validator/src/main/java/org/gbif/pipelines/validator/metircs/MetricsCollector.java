package org.gbif.pipelines.validator.metircs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.factory.ElasticsearchClientFactory;
import org.gbif.pipelines.validator.metircs.es.OccurrenceIssuesRequestBuilder;
import org.gbif.pipelines.validator.metircs.es.TermCountRequestBuilder;
import org.gbif.pipelines.validator.metircs.es.TermCountRequestBuilder.TermCountRequest;

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
    Map<String, Long> coreTermCountMap = queryTermsCount(corePrefix, coreTerms);

    // Query ES  - all extensions terms
    Map<String, Map<String, Long>> extensionsTermsCountMap = new HashMap<>();
    extensionsTerms.forEach(
        (key, value) -> {
          String extPrefix = extensionsPrefix + "." + key.getRowType();
          Map<String, Long> extTermCountMap = queryTermsCount(extPrefix, value);
          extensionsTermsCountMap.put(key.getRowType(), extTermCountMap);
        });

    // Query ES - aggregate OccurrenceIssues
    Map<String, Long> occurrenceIssues = queryOccurrenceIssues();

    return Metrics.builder()
        .coreTermsCountMap(coreTermCountMap)
        .extensionsTermsCountMap(extensionsTermsCountMap)
        .occurrenceIssuesMap(occurrenceIssues)
        .build();
  }

  private Map<String, Long> queryTermsCount(String prefix, Set<Term> terms) {

    return terms.stream()
        .parallel()
        .map(term -> buildCountRequest(prefix, term))
        .collect(
            Collectors.toMap(t -> t.getTerm().qualifiedName(), t -> getCount(t.getCountRequest())));
  }

  private TermCountRequest buildCountRequest(String prefix, Term term) {
    return TermCountRequestBuilder.builder()
        .prefix(prefix)
        .datasetKey(datasetKey)
        .indexName(index)
        .build()
        .getRequest(term);
  }

  @SneakyThrows
  private Map<String, Long> queryOccurrenceIssues() {
    SearchRequest request =
        OccurrenceIssuesRequestBuilder.builder()
            .datasetKey(datasetKey)
            .indexName(index)
            .build()
            .getRequest();

    Aggregation aggregation =
        ElasticsearchClientFactory.getInstance(esHost)
            .search(request, RequestOptions.DEFAULT)
            .getAggregations()
            .get(OccurrenceIssuesRequestBuilder.AGGREGATION);

    return ((ParsedStringTerms) aggregation)
        .getBuckets().stream()
            .collect(Collectors.toMap(Bucket::getKeyAsString, Bucket::getDocCount, (a1, b) -> b));
  }

  @SneakyThrows
  private long getCount(CountRequest countRequest) {
    return ElasticsearchClientFactory.getInstance(esHost)
        .count(countRequest, RequestOptions.DEFAULT)
        .getCount();
  }
}
