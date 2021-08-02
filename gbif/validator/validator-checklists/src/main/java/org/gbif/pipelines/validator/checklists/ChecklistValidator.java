package org.gbif.pipelines.validator.checklists;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.checklistbank.cli.normalizer.Normalizer;
import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.checklistbank.neo.UsageDao;
import org.gbif.nub.lookup.straight.IdLookupPassThru;
import org.neo4j.graphdb.Transaction;

/**
 * Evaluates checklists using ChecklistBank Normalizer. Currently, no nub matching is done. Not
 * Thread-Safe.
 */
public class ChecklistValidator {

  @Data
  @Builder
  public static class ChecklistValidationResult {
    private final NameUsage nameUsage;
    private final VerbatimNameUsage verbatimNameUsage;
  }

  private final NormalizerConfiguration configuration;

  /**
   * @param configuration
   * @param workingFolder where temporary results will be stored. The called is responsible to
   *     delete it.
   */
  public ChecklistValidator(NormalizerConfiguration configuration, Path workingFolder) {
    this.configuration = new NormalizerConfiguration();

    // use our own neo repository
    this.configuration.neo.neoRepository = workingFolder.resolve("neo").toFile();

    // copy other known configuration
    this.configuration.neo.batchSize = configuration.neo.batchSize;
    this.configuration.neo.mappedMemory = configuration.neo.mappedMemory;
    this.configuration.poolSize = configuration.poolSize;
  }

  /**
   * The NormalizerConfiguration instance is used to run a single Normalizer each time this method
   * is executed.
   *
   * @throws IOException
   */
  public List<ChecklistValidationResult> evaluate(Path archivePath) {

    // The generated a random dataset key, we only need it as a key
    UUID datasetKey = UUID.randomUUID();

    try (UsageDao dao = UsageDao.create(configuration.neo, datasetKey)) {
      Normalizer normalizer =
          Normalizer.create(
              datasetKey,
              dao,
              archivePath.toFile(),
              new IdLookupPassThru(),
              configuration.neo.batchSize);
      normalizer.run(false);
      return collectUsagesData(dao);
    }
  }

  /** Collect issues and graph data from the normalization result. */
  private List<ChecklistValidationResult> collectUsagesData(UsageDao dao) {
    try (Transaction tx = dao.beginTx()) {
      // iterate over all node and collect their issues
      return dao.allNodes().stream()
          .map(
              node ->
                  ChecklistValidationResult.builder()
                      .nameUsage(dao.readUsage(node, false))
                      .verbatimNameUsage(dao.readVerbatim(node.getId()))
                      .build())
          .collect(Collectors.toList());
    }
  }
}
