package org.gbif.pipelines.validator.metircs;

import java.time.ZonedDateTime;
import java.util.List;
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
import org.gbif.pipelines.validator.ValidationStatus;
import org.gbif.pipelines.validator.factory.ElasticsearchClientFactory;
import org.gbif.pipelines.validator.metircs.Metrics.Result;
import org.gbif.pipelines.validator.metircs.request.OccurrenceIssuesRequestBuilder;
import org.gbif.pipelines.validator.metircs.request.TermCountRequestBuilder;
import org.gbif.pipelines.validator.metircs.request.TermCountRequestBuilder.TermCountRequest;

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

    // Core
    Metrics.Core core =
        Metrics.Core.builder()
            .indexedCount(1L)
            .indexedCoreTerm(queryTermsCount(corePrefix, coreTerms))
            .occurrenceIssuesMap(queryOccurrenceIssues())
            .build();

    // Extensions
    List<Metrics.Extension> extensions =
        extensionsTerms.entrySet().stream()
            .map(
                es -> {
                  String extPrefix = extensionsPrefix + "." + es.getKey().getRowType();
                  return Metrics.Extension.builder()
                      .rowType(es.getKey().getRowType())
                      .indexedCount(queryDocCount())
                      .extensionsTermsCountMap(queryTermsCount(extPrefix, es.getValue()))
                      .build();
                })
            .collect(Collectors.toList());

    return Metrics.builder()
        .datasetKey(datasetKey)
        .status(ValidationStatus.FINISHED)
        .endTimestamp(ZonedDateTime.now().toEpochSecond())
        .result(Result.builder().core(core).extensions(extensions).build())
        .build();
  }

  private Long queryDocCount() {
    return getCount(buildCountRequest(null, null).getCountRequest());
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
