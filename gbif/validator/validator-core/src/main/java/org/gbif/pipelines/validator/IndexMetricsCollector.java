package org.gbif.pipelines.validator;

import static org.gbif.dwc.terms.DwcTerm.Event;
import static org.gbif.dwc.terms.DwcTerm.Occurrence;
import static org.gbif.pipelines.validator.metrics.request.OccurrenceIssuesRequestBuilder.HITS_AGGREGATION;
import static org.gbif.pipelines.validator.metrics.request.OccurrenceIssuesRequestBuilder.ISSUES_AGGREGATION;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.validator.factory.ElasticsearchClientFactory;
import org.gbif.pipelines.validator.metrics.request.ExtensionTermCountRequestBuilder;
import org.gbif.pipelines.validator.metrics.request.ExtensionTermCountRequestBuilder.ExtTermCountRequest;
import org.gbif.pipelines.validator.metrics.request.OccurrenceIssuesRequestBuilder;
import org.gbif.pipelines.validator.metrics.request.TermCountRequestBuilder;
import org.gbif.pipelines.validator.metrics.request.TermCountRequestBuilder.TermCountRequest;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.IssueSample;
import org.gbif.validator.api.Metrics.TermInfo;

/**
 * The class collects all necessary metrics using ES API, there are 4 main queries. such as:
 *
 * <pre>
 * 1) Query total documents count
 * 2) Query core terms and return term, counts of raw and indexed terms
 * 3) Query extensions terms and return term, and raw terms count
 * 4) Query all issues and return issue value, and 5 terms samples
 * </pre>
 */
@Slf4j
@Builder
public class IndexMetricsCollector {

  private final String[] esHost;
  private final List<FileInfo> fileInfos;
  private final UUID key;
  private final String index;
  private final String corePrefix;
  private final String extensionsPrefix;

  /** Collect all metrics using ES API */
  public Metrics collect() {

    fileInfos.stream()
        .filter(f -> f.getRowType() != null)
        .forEach(
            fileInfo -> {
              if (fileInfo.getRowType().equals(Occurrence.qualifiedName())) {
                collectOccurrnceInfo(fileInfo);
              } else if (fileInfo.getRowType().equals(Event.qualifiedName())
                  && fileInfo.getFileType() == DwcFileType.CORE) {
                collectEventInfo(fileInfo);
              } else if (!fileInfo.getRowType().equals(Occurrence.qualifiedName())
                  && fileInfo.getFileType() == DwcFileType.EXTENSION) {
                collectExtensionInfo(fileInfo);
              }
            });

    return Metrics.builder().fileInfos(fileInfos).build();
  }

  private void collectOccurrnceInfo(FileInfo fileInfo) {
    fileInfo.setIndexedCount(queryOccurrenceDocCount());
    fileInfo.setIssues(queryOccurrenceIssuesCount());
    for (TermInfo ti : fileInfo.getTerms()) {
      queryOccurrenceCoreTermsCount(ti.getTerm()).ifPresent(ti::setInterpretedIndexed);
    }
  }

  private void collectEventInfo(FileInfo fileInfo) {
    fileInfo.setIndexedCount(queryOccurrenceDocCount());
    for (TermInfo ti : fileInfo.getTerms()) {
      queryOccurrenceCoreTermsCount(ti.getTerm()).ifPresent(ti::setInterpretedIndexed);
    }
  }

  private void collectExtensionInfo(FileInfo fileInfo) {
    String extPrefix = extensionsPrefix + "." + fileInfo.getRowType();
    for (TermInfo ti : fileInfo.getTerms()) {
      ti.setInterpretedIndexed(queryExtensionTermsCount(extPrefix, ti.getTerm()));
    }
  }

  /** Query indexed document count by datasetKey */
  @SneakyThrows
  private Long queryOccurrenceDocCount() {
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

  /** Aggregate all issues and return 5 samples per issue */
  @SneakyThrows
  private List<IssueInfo> queryOccurrenceIssuesCount() {
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
            .get(ISSUES_AGGREGATION);

    return ((ParsedStringTerms) aggregation)
        .getBuckets().stream().map(this::collectIssueInfo).collect(Collectors.toList());
  }

  /** Process one issue bucket, get issue value, count and 5 samples of related data */
  private IssueInfo collectIssueInfo(Bucket bucket) {
    SearchHit[] hits =
        ((ParsedTopHits) bucket.getAggregations().get(HITS_AGGREGATION)).getHits().getHits();

    String occurrenceIssueString = bucket.getKeyAsString();

    List<IssueSample> issueSamples = new ArrayList<>(hits.length);
    for (SearchHit hit : hits) {
      // Get core object map from verbatim record
      Map<String, String> core =
          ((HashMap<String, HashMap<String, String>>) hit.getSourceAsMap().get(Indexing.VERBATIM))
              .get(Indexing.CORE);

      // Get id of the record
      String id = (String) hit.getSourceAsMap().get(Indexing.ID);

      // Find related terms
      Set<Term> terms = Collections.emptySet();
      try {
        OccurrenceIssue issue = OccurrenceIssue.valueOf(occurrenceIssueString);
        terms = issue.getRelatedTerms();
      } catch (IllegalArgumentException ex) {
        log.warn("Can't find enum value for OccurrenceIssue - {}", occurrenceIssueString);
      }

      // Find values of the related terms
      Map<String, String> relatedData = new HashMap<>();
      for (Term term : terms) {
        String s = core.get(term.qualifiedName());
        if (s != null && !s.trim().isEmpty()) {
          relatedData.put(term.toString(), s);
        }
      }

      issueSamples.add(IssueSample.builder().recordId(id).relatedData(relatedData).build());
    }

    return IssueInfo.builder()
        .issue(occurrenceIssueString)
        .count(bucket.getDocCount())
        .samples(issueSamples)
        .issueCategory(EvaluationCategory.OCC_INTERPRETATION_BASED)
        .build();
  }

  /** Aggregate occurrence terms and return term count */
  @SneakyThrows
  private Optional<Long> queryOccurrenceCoreTermsCount(String term) {

    TermCountRequest tcr =
        TermCountRequestBuilder.builder()
            .termValue(key.toString())
            .prefix(corePrefix)
            .indexName(index)
            .term(term)
            .build()
            .getRequest();

    Optional<CountRequest> countRequest = tcr.getInterpretedCountRequest();
    Long interpretedCount = null;
    if (countRequest.isPresent()) {
      interpretedCount =
          ElasticsearchClientFactory.getInstance(esHost)
              .count(countRequest.get(), RequestOptions.DEFAULT)
              .getCount();
    }

    return Optional.ofNullable(interpretedCount);
  }

  /** Aggregate extensions term and return term count */
  @SneakyThrows
  private Long queryExtensionTermsCount(String prefix, String term) {
    ExtTermCountRequest etcr =
        ExtensionTermCountRequestBuilder.builder()
            .prefix(prefix)
            .termValue(key.toString())
            .indexName(index)
            .term(term)
            .build()
            .getRequest();

    Aggregation aggregation =
        ElasticsearchClientFactory.getInstance(esHost)
            .search(etcr.getSearchRequest(), RequestOptions.DEFAULT)
            .getAggregations()
            .get(ExtensionTermCountRequestBuilder.AGGREGATION);

    return ((ParsedValueCount) aggregation).getValue();
  }
}
