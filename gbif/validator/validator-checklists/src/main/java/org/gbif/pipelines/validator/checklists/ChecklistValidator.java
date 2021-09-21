package org.gbif.pipelines.validator.checklists;

import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Collectors;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.checklistbank.cli.normalizer.Normalizer;
import org.gbif.checklistbank.neo.UsageDao;
import org.gbif.nub.lookup.straight.IdLookupPassThru;
import org.gbif.validator.api.Metrics;
import org.neo4j.graphdb.Transaction;

/**
 * Evaluates checklists using ChecklistBank Normalizer. Currently, no nub matching is done. Not
 * Thread-Safe.
 */
public class ChecklistValidator {

  private final NeoConfiguration configuration;

  /** @param neoConfiguration Neo4j configuration. */
  public ChecklistValidator(NeoConfiguration neoConfiguration) {
    // use our own neo repository
    this.configuration = neoConfiguration;
  }

  /**
   * The NormalizerConfiguration instance is used to run a single Normalizer each time this method
   * is executed.
   */
  public Metrics.ChecklistValidationReport evaluate(Path archivePath) {

    // The generated a random dataset key, we only need it as a key
    UUID datasetKey = UUID.randomUUID();

    try (UsageDao dao = UsageDao.create(configuration, datasetKey)) {
      Normalizer normalizer =
          Normalizer.create(
              datasetKey,
              dao,
              archivePath.toFile(),
              new IdLookupPassThru(),
              configuration.batchSize);
      normalizer.run(false);
      return collectUsagesData(dao);
    }
  }

  /** Collect issues and graph data from the normalization result. */
  private Metrics.ChecklistValidationReport collectUsagesData(UsageDao dao) {
    try (Transaction tx = dao.beginTx()) {
      // iterate over all node and collect their issues
      return Metrics.ChecklistValidationReport.builder()
          .results(
              dao.allNodes().stream()
                  .map(
                      node ->
                          Metrics.ChecklistValidationReport.ChecklistValidationResult.builder()
                              .nameUsage(dao.readUsage(node, false))
                              .verbatimNameUsage(dao.readVerbatim(node.getId()))
                              .build())
                  .collect(Collectors.toList()))
          .build();
    }
  }
}
