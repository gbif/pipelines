package org.gbif.pipelines.validator.checklists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
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
import org.neo4j.graphdb.Transaction;

/**
 * Evaluates checklists using ChecklistBank Normalizer. Currently, no nub matching is done. Not
 * Thread-Safe.
 */
@Slf4j
public class ChecklistValidator {

  private final NeoConfiguration configuration;

  /** @param neoConfiguration Neo4j configuration. */
  public ChecklistValidator(NeoConfiguration neoConfiguration) {
    // use our own neo repository
    this.configuration = neoConfiguration;
  }

  /**
   * BY using the Checklist Normalizer collects the issues for all Taxon file core or extensions.
   */
  @SneakyThrows
  public List<Metrics.FileInfo> evaluate(Path archivePath) {
    Archive archive = DwcFiles.fromLocation(archivePath);
    List<Metrics.FileInfo> results = new ArrayList<>();
    ArchiveFile core = archive.getCore();
    if (core.getRowType() == DwcTerm.Taxon) {
      results.add(validateTaxonArchive(core, DwcFileType.CORE));
    }
    archive
        .getExtensions()
        .forEach(
            extArchive -> {
              if (extArchive.getRowType() == DwcTerm.Taxon) {
                results.add(validateTaxonArchive(extArchive, DwcFileType.EXTENSION));
              }
            });
    return results;
  }

  /** Validates a ArchiveFile that checklist data. */
  private Metrics.FileInfo validateTaxonArchive(ArchiveFile archiveFile, DwcFileType dwcFileType) {
    return Metrics.FileInfo.builder()
        .rowType(DwcTerm.Taxon.simpleName())
        .count(countLines(archiveFile.getLocationFile(), areHeaderLinesIncluded(archiveFile)))
        .fileName(archiveFile.getLocationFile().getName())
        .fileType(dwcFileType)
        .issues(collectIssuesInfo(archiveFile))
        .build();
  }

  /** Efficient way of counting lines */
  private long countLines(File file, boolean areHeaderLinesIncluded) {
    long lines = areHeaderLinesIncluded ? -1 : 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      while (reader.readLine() != null) {
        lines++;
      }
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
    }
    return lines;
  }

  /** Exclude header from counter */
  private boolean areHeaderLinesIncluded(ArchiveFile archiveFile) {
    return archiveFile.getIgnoreHeaderLines() != null && archiveFile.getIgnoreHeaderLines() > 0;
  }

  /** Executes the Checklistbank normalizer and collects all data using a BiConsumer. */
  private void runNormalizer(
      ArchiveFile archiveFile, BiConsumer<NameUsage, VerbatimNameUsage> resultCollector) {
    UUID key = UUID.randomUUID();
    try (UsageDao dao = UsageDao.create(configuration, key)) {
      Normalizer normalizer =
          Normalizer.create(
              key,
              dao,
              archiveFile.getLocationFile().getParentFile(),
              new IdLookupPassThru(),
              configuration.batchSize);
      normalizer.run(false);
      try (Transaction tx = dao.beginTx()) {
        // iterate over all node and collect their issues
        dao.allNodes()
            .forEach(
                node ->
                    resultCollector.accept(
                        dao.readUsage(node, false), dao.readVerbatim(node.getId())));
      }
    }
  }

  /** Collect issues and graph data from the normalization result. */
  private List<Metrics.IssueInfo> collectIssuesInfo(ArchiveFile archiveFile) {
    IssueInfoCollector collector = new IssueInfoCollector();
    runNormalizer(archiveFile, collector::collect);
    return collector.getIssuesInfo();
  }

  /** Utility class to collect and summarize information from NameUsages and VerbatimUsages. */
  public static class IssueInfoCollector {

    private static final int SAMPLE_SIZE = 5;

    private final Map<String, Metrics.IssueInfo> issueInfoMap = new HashMap<>();

    /** Collects the IssueInfo from the NameUsage and VerbatimUsage. */
    public void collect(NameUsage nameUsage, VerbatimNameUsage verbatimNameUsage) {
      if (nameUsage.getIssues() != null) {
        nameUsage.getIssues().forEach(issue -> addOrCreateIssueInfo(issue, verbatimNameUsage));
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

    /** Returns the list of IssuesInfo collected so far. */
    public List<Metrics.IssueInfo> getIssuesInfo() {
      return new ArrayList<>(issueInfoMap.values());
    }

    private static Map<String, String> getRelatedData(
        NameUsageIssue issue, VerbatimNameUsage verbatimNameUsage) {
      return issue.getRelatedTerms().stream()
          .filter(t -> verbatimNameUsage.getCoreField(t) != null)
          .collect(Collectors.toMap(Term::simpleName, verbatimNameUsage::getCoreField));
    }
  }
}
