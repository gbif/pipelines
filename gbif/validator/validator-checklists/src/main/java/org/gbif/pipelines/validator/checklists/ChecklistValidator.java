package org.gbif.pipelines.validator.checklists;

import static org.gbif.pipelines.validator.checklists.ArchiveUtils.areHeaderLinesIncluded;
import static org.gbif.pipelines.validator.checklists.ArchiveUtils.countLines;

import com.google.common.collect.Sets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.checklistbank.cli.normalizer.Normalizer;
import org.gbif.checklistbank.neo.UsageDao;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.nub.lookup.straight.IdLookupPassThru;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Evaluates checklists using ChecklistBank Normalizer. Currently, no nub matching is done. Not
 * Thread-Safe.
 */
@Slf4j
public class ChecklistValidator {

  private static final Set<String> NAME_USAGES_RELATED_EXTENSIONS =
      Sets.newHashSet(
          Extension.DISTRIBUTION.getRowType(),
          Extension.DESCRIPTION.getRowType(),
          Extension.IDENTIFIER.getRowType(),
          Extension.REFERENCE.getRowType(),
          Extension.VERNACULAR_NAME.getRowType(),
          Extension.TYPES_AND_SPECIMEN.getRowType(),
          Extension.SPECIES_PROFILE.getRowType(),
          Extension.MULTIMEDIA.getRowType(),
          DwcTerm.Taxon.qualifiedName());

  private final NeoConfiguration configuration;

  /** @param neoConfiguration Neo4j configuration. */
  public ChecklistValidator(NeoConfiguration neoConfiguration) {
    // use our own neo repository
    this.configuration = neoConfiguration;
  }

  /**
   * By using the Checklist Normalizer collects the issues for all Taxon file core or extensions.
   */
  @SneakyThrows
  public List<Metrics.FileInfo> evaluate(Path archivePath) {
    Archive archive = DwcFiles.fromLocation(archivePath);
    List<Metrics.FileInfo> results = new ArrayList<>();
    ArchiveFile core = archive.getCore();
    if (core.getRowType() == DwcTerm.Taxon) {
      results.addAll(validateArchive(archive));
    }
    return results;
  }

  /** Validates a ArchiveFile that checklist data. */
  private List<Metrics.FileInfo> validateArchive(Archive archive) {
    ValidationDataCollector collector = validate(archive);
    List<Metrics.FileInfo> results = new ArrayList<>();
    results.add(
        Metrics.FileInfo.builder()
            .rowType(DwcTerm.Taxon.simpleName())
            .count(
                countLines(
                    archive.getCore().getLocationFile(), areHeaderLinesIncluded(archive.getCore())))
            .fileName(archive.getCore().getLocationFile().getName())
            .fileType(DwcFileType.CORE)
            .issues(collector.getIssuesInfo())
            .terms(collector.getTermInfo())
            .indexedCount(collector.getUsagesCount())
            .build());
    results.addAll(collectExtensionsData(archive, collector));
    return results;
  }

  public List<Metrics.FileInfo> collectExtensionsData(
      Archive archive, ValidationDataCollector collector) {
    if (archive.getExtensions() != null) {
      return archive.getExtensions().stream()
          .filter(
              extension ->
                  NAME_USAGES_RELATED_EXTENSIONS.contains(extension.getRowType().qualifiedName()))
          .map(
              extension ->
                  Metrics.FileInfo.builder()
                      .rowType(extension.getRowType().qualifiedName())
                      .count(
                          countLines(
                              extension.getLocationFile(),
                              areHeaderLinesIncluded(archive.getCore())))
                      .fileName(extension.getLocationFile().getName())
                      .fileType(DwcFileType.EXTENSION)
                      .terms(
                          collector.getExtensionTermInfo(
                              Extension.fromRowType(extension.getRowType().qualifiedName())))
                      .indexedCount(collector.getUsagesCount())
                      .build())
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /** Executes the Checklistbank normalizer and collects all data using a BiConsumer. */
  private ValidationDataCollector validate(Archive archive) {
    ValidationDataCollector collector = new ValidationDataCollector();
    UUID key = UUID.randomUUID();
    try (UsageDao dao = UsageDao.create(configuration, key)) {
      Normalizer normalizer =
          Normalizer.create(
              key, dao, archive.getLocation(), new IdLookupPassThru(), configuration.batchSize);
      normalizer.run(false);
      try (Transaction tx = dao.beginTx()) {
        // iterate over all node and collect their issues
        dao.allNodes().forEach(node -> collector.collect(readUsageData(dao, node)));
      }
    }
    return collector;
  }

  /** Collects usages data from the DAO object. */
  private NormalizedNameUsageData readUsageData(UsageDao usageDao, Node node) {
    return NormalizedNameUsageData.builder()
        .nameUsage(usageDao.readUsage(node, true))
        .verbatimNameUsage(usageDao.readVerbatim(node.getId()))
        .parsedName(usageDao.readName(node.getId()))
        .usageExtensions(usageDao.readExtensions(node.getId()))
        .build();
  }

  /** Utility class to collect and summarize information from NameUsages and VerbatimUsages. */
  public static class ValidationDataCollector {

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
                issue ->
                    addOrCreateIssueInfo(issue, normalizedNameUsageData.getVerbatimNameUsage()));
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
                  Metrics.IssueSample.builder()
                      .relatedData(getRelatedData(issue, verbatimNameUsage));
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
    private void collect(NormalizedNameUsageData normalizedNameUsageData) {
      termFrequencyCollector.add(TermFrequencyCollector.of(normalizedNameUsageData));
      collectIssuesInfo(normalizedNameUsageData);
      usagesCount = usagesCount + 1;
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

    private static Map<String, String> getRelatedData(
        NameUsageIssue issue, VerbatimNameUsage verbatimNameUsage) {
      return issue.getRelatedTerms().stream()
          .filter(t -> verbatimNameUsage.getCoreField(t) != null)
          .collect(Collectors.toMap(Term::simpleName, verbatimNameUsage::getCoreField));
    }
  }
}
