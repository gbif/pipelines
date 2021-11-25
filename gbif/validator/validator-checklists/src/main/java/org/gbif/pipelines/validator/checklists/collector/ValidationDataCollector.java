package org.gbif.pipelines.validator.checklists.collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.api.vocabulary.Origin;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.checklists.model.NormalizedNameUsageData;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;

/** Utility class to collect and summarize information from NameUsages and VerbatimUsages. */
public class ValidationDataCollector {

  private static final int SAMPLE_SIZE = 5;

  private final Map<String, Metrics.IssueInfo> issueInfoMap = new HashMap<>();

  private Long usagesCount = 0L;

  private final TermFrequencyCollector termFrequencyCollector = new TermFrequencyCollector();

  /** Collects the IssueInfo from the NameUsage and VerbatimUsage. */
  public void collectIssuesInfo(NormalizedNameUsageData normalizedNameUsageData) {
    if (normalizedNameUsageData.getNameUsage().getIssues() != null) {
      normalizedNameUsageData
          .getNameUsage()
          .getIssues()
          .forEach(
              issue -> addOrCreateIssueInfo(issue, normalizedNameUsageData.getVerbatimNameUsage()));
    }
  }

  /** Adds a new IssueInfo or creates a new one to the issueInfoMap. */
  private void addOrCreateIssueInfo(NameUsageIssue issue, VerbatimNameUsage verbatimNameUsage) {
    issueInfoMap.compute(
        issue.name(),
        (k, v) -> {
          if (v == null) {
            return Metrics.IssueInfo.builder()
                .issue(k)
                .issueCategory(EvaluationCategory.CLB_INTERPRETATION_BASED)
                .count(1L)
                .samples(new ArrayList<>()) // Needs to be initialized here to avoid an empty
                // unmodifiable list
                .build();
          }
          if (v.getSamples().size() < SAMPLE_SIZE) {
            Metrics.IssueSample.IssueSampleBuilder builder =
                Metrics.IssueSample.builder().relatedData(getRelatedData(issue, verbatimNameUsage));
            if (verbatimNameUsage.hasCoreField(DwcTerm.taxonID)) {
              builder.recordId(verbatimNameUsage.getCoreField(DwcTerm.taxonID));
            }
            v.getSamples().add(builder.build());
          }
          v.setCount(v.getCount() + 1);
          return v;
        });
  }

  /** Adds a new TermInfo or creates a new one to the termInfoMap. */
  public void collect(NormalizedNameUsageData normalizedNameUsageData) {
    if (normalizedNameUsageData.getNameUsage() != null
        && Origin.SOURCE == normalizedNameUsageData.getNameUsage().getOrigin()) {
      synchronized (this) {
        termFrequencyCollector.add(TermFrequencyCollector.of(normalizedNameUsageData));
        collectIssuesInfo(normalizedNameUsageData);
        usagesCount = usagesCount + 1;
      }
    }
  }

  public List<Metrics.IssueInfo> getIssuesInfo() {
    return new ArrayList<>(this.issueInfoMap.values());
  }

  public List<Metrics.TermInfo> getTermInfo() {
    return termFrequencyCollector.toTermsInfo();
  }

  public List<Metrics.TermInfo> getExtensionTermInfo(Extension extension) {
    return termFrequencyCollector.toExtensionTermInfo(extension);
  }

  /** Total number of name usages processed. */
  public Long getUsagesCount() {
    return usagesCount;
  }

  public Long getVerbatimExtensionRowCount(Extension extension) {
    return termFrequencyCollector.getVerbatimExtensionRowCount(extension);
  }

  public Long getInterpretedExtensionRowCount(Extension extension) {
    return termFrequencyCollector.getInterpretedExtensionRowCount(extension);
  }

  private static Map<String, String> getRelatedData(
      NameUsageIssue issue, VerbatimNameUsage verbatimNameUsage) {
    return issue.getRelatedTerms().stream()
        .filter(t -> verbatimNameUsage.getCoreField(t) != null)
        .collect(Collectors.toMap(Term::simpleName, verbatimNameUsage::getCoreField));
  }
}
