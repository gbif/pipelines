package org.gbif.pipelines.validator.checklists;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Extension;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.checklistbank.cli.normalizer.Normalizer;
import org.gbif.checklistbank.neo.UsageDao;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.nub.lookup.straight.IdLookup;
import org.gbif.nub.lookup.straight.IdLookupPassThru;
import org.gbif.pipelines.validator.checklists.collector.ValidationDataCollector;
import org.gbif.pipelines.validator.checklists.model.NormalizedNameUsageData;
import org.gbif.pipelines.validator.checklists.ws.IdLookupClient;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;

/**
 * Evaluates checklists using ChecklistBank Normalizer. Currently, no nub matching is done. Not
 * Thread-Safe.
 */
@Slf4j
public class ChecklistValidator implements Closeable {

  @Data
  @Builder
  public static class Configuration {

    private final NeoConfiguration neoConfiguration;

    private final String apiUrl;
  }

  private static final Set<String> NAME_USAGES_RELATED_EXTENSIONS =
      new HashSet<>(
          Arrays.asList(
              Extension.DISTRIBUTION.getRowType(),
              Extension.DESCRIPTION.getRowType(),
              Extension.IDENTIFIER.getRowType(),
              Extension.REFERENCE.getRowType(),
              Extension.VERNACULAR_NAME.getRowType(),
              Extension.TYPES_AND_SPECIMEN.getRowType(),
              Extension.SPECIES_PROFILE.getRowType(),
              Extension.MULTIMEDIA.getRowType(),
              DwcTerm.Taxon.qualifiedName()));

  private final Configuration configuration;

  private final IdLookup idLookup;

  /**
   * @param configuration Neo4j configuration.
   */
  public ChecklistValidator(Configuration configuration) {
    // use our own neo repository
    this.configuration = configuration;
    idLookup =
        configuration.apiUrl == null
            ? new IdLookupPassThru()
            : new IdLookupClient(configuration.apiUrl);
  }

  /**
   * By using the Checklist Normalizer collects the issues for all Taxon file core or extensions.
   */
  public List<Metrics.FileInfo> evaluate(Path archivePath) throws IOException {
    Archive archive = DwcFiles.fromLocation(archivePath);
    List<Metrics.FileInfo> results = new ArrayList<>();
    ArchiveFile core = archive.getCore();
    if (core.getRowType() == DwcTerm.Taxon) {
      results.addAll(validateArchive(archive));
    }
    return results;
  }

  @SneakyThrows
  @Override
  public void close() {
    idLookup.close();
  }

  /** Validates a ArchiveFile that checklist data. */
  private List<Metrics.FileInfo> validateArchive(Archive archive) {
    ValidationDataCollector collector = validate(archive);
    List<Metrics.FileInfo> results = new ArrayList<>();
    results.add(
        Metrics.FileInfo.builder()
            .rowType(DwcTerm.Taxon.qualifiedName())
            .count(LineCounter.count(archive.getCore()))
            .fileName(archive.getCore().getFirstLocationFile().getName())
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
              extension -> {
                Extension nameUsageExtension =
                    Extension.fromRowType(extension.getRowType().qualifiedName());
                return Metrics.FileInfo.builder()
                    .rowType(extension.getRowType().qualifiedName())
                    .count(LineCounter.count(extension))
                    .fileName(extension.getFirstLocationFile().getName())
                    .fileType(DwcFileType.EXTENSION)
                    .terms(collector.getExtensionTermInfo(nameUsageExtension))
                    .indexedCount(collector.getInterpretedExtensionRowCount(nameUsageExtension))
                    .build();
              })
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /** Executes the Checklistbank normalizer and collects all data using a BiConsumer. */
  private ValidationDataCollector validate(Archive archive) {
    ValidationDataCollector collector = new ValidationDataCollector();
    UUID key = UUID.randomUUID();
    try (UsageDao dao = UsageDao.create(configuration.neoConfiguration, key)) {
      Normalizer normalizer =
          Normalizer.create(
              key, dao, archive.getLocation(), idLookup, configuration.neoConfiguration.batchSize);
      normalizer.run(false);
      // iterate over all node and collect their issues
      dao.streamUsages().parallel().forEach(node -> collector.collect(readUsageData(node)));
    }
    return collector;
  }

  /** Collects usages data from the DAO object. */
  private NormalizedNameUsageData readUsageData(UsageDao.FullUsage node) {
    return NormalizedNameUsageData.builder()
        .nameUsage(node.usage)
        .verbatimNameUsage(node.verbatim)
        .parsedName(node.name)
        .usageExtensions(node.extensions)
        .build();
  }
}
