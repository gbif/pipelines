package org.gbif.pipelines.validator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.factory.ElasticsearchClientFactory;
import org.gbif.pipelines.validator.metircs.request.ExtensionTermCountRequestBuilder;
import org.gbif.pipelines.validator.metircs.request.ExtensionTermCountRequestBuilder.ExtTermCountRequest;
import org.gbif.pipelines.validator.metircs.request.OccurrenceIssuesRequestBuilder;
import org.gbif.pipelines.validator.metircs.request.TermCountRequestBuilder;
import org.gbif.pipelines.validator.metircs.request.TermCountRequestBuilder.TermCountRequest;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.Core.IssueInfo;
import org.gbif.validator.api.Metrics.Core.TermInfo;

// TODO: DOC
@Builder
public class MetricsCollector {

  private final String[] esHost;
  private final Set<Term> coreTerms;
  private final Map<Extension, Set<Term>> extensionsTerms;
  private final UUID key;
  private final String index;
  private final String corePrefix;
  private final String extensionsPrefix;

  // TODO: DOC
  public Metrics collect() {

    // Core
    Metrics.Core core =
        Metrics.Core.builder()
            .indexedCount(queryDocCount())
            .indexedCoreTerms(queryCoreTermsCount())
            .occurrenceIssues(queryOccurrenceIssuesCount())
            .build();

    // Extensions
    List<Metrics.Extension> extensions =
        extensionsTerms.entrySet().stream()
            .filter(es -> es.getKey() != null)
            .map(
                es -> {
                  String extPrefix = extensionsPrefix + "." + es.getKey().getRowType();
                  return Metrics.Extension.builder()
                      .rowType(es.getKey().getRowType())
                      .extensionsTermsCounts(queryExtTermsCount(extPrefix, es.getValue()))
                      .build();
                })
            .collect(Collectors.toList());

    return Metrics.builder().core(core).extensions(extensions).build();
  }

  @SneakyThrows
  private Long queryDocCount() {
    TermCountRequest request =
        TermCountRequestBuilder.builder()
            .termValue(key.toString())
            .indexName(index)
            .build()
            .getRequest();

    return ElasticsearchClientFactory.getInstance(esHost)
        .count(request.getRawCountRequest(), RequestOptions.DEFAULT)
        .getCount();
  }

  // TODO: DOC
  private Set<TermInfo> queryCoreTermsCount() {

    Function<Term, TermCountRequest> requestFn =
        term ->
            TermCountRequestBuilder.builder()
                .termValue(key.toString())
                .prefix(corePrefix)
                .indexName(index)
                .term(term)
                .build()
                .getRequest();

    Function<TermCountRequest, TermInfo> countFn =
        tcr -> {
          try {
            Long rawCount =
                ElasticsearchClientFactory.getInstance(esHost)
                    .count(tcr.getRawCountRequest(), RequestOptions.DEFAULT)
                    .getCount();

            Long interpretedCount = null;
            if (tcr.getInterpretedCountRequest().isPresent()) {
              interpretedCount =
                  ElasticsearchClientFactory.getInstance(esHost)
                      .count(tcr.getInterpretedCountRequest().get(), RequestOptions.DEFAULT)
                      .getCount();
            }

            return TermInfo.builder()
                .term(tcr.getTerm().qualifiedName())
                .rawIndexed(rawCount)
                .interpretedIndexed(interpretedCount)
                .build();

          } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
          }
        };

    return coreTerms.stream().parallel().map(requestFn).map(countFn).collect(Collectors.toSet());
  }

  // TODO: DOC
  private Map<String, Long> queryExtTermsCount(String prefix, Set<Term> terms) {

    Function<Term, ExtTermCountRequest> requestFn =
        (term) ->
            ExtensionTermCountRequestBuilder.builder()
                .prefix(prefix)
                .termValue(key.toString())
                .indexName(index)
                .term(term)
                .build()
                .getRequest();

    Function<ExtTermCountRequest, Long> countFn =
        extTermCountRequest -> {
          try {
            Aggregation aggregation =
                ElasticsearchClientFactory.getInstance(esHost)
                    .search(extTermCountRequest.getSearchRequest(), RequestOptions.DEFAULT)
                    .getAggregations()
                    .get(ExtensionTermCountRequestBuilder.AGGREGATION);
            return ((ParsedValueCount) aggregation).getValue();
          } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
          }
        };

    return terms.stream()
        .parallel()
        .map(requestFn)
        .collect(Collectors.toMap(t -> t.getTerm().qualifiedName(), countFn));
  }

  // TODO: DOC
  @SneakyThrows
  private Set<IssueInfo> queryOccurrenceIssuesCount() {
    SearchRequest request =
        OccurrenceIssuesRequestBuilder.builder()
            .termValue(key.toString())
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
            .map(
                b -> {
                  // TODO: GET RELATED TERMS SAMPLES!
                  // SearchHit[] hits = ((ParsedTopHits)
                  // b.getAggregations().get("by_hits")).getHits().getHits();
                  return IssueInfo.builder()
                      .issue(b.getKeyAsString())
                      .count(b.getDocCount())
                      .build();
                })
            .collect(Collectors.toSet());
  }
}
