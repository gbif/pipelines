package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.client.EsConfig;
import org.gbif.pipelines.esindexing.common.SettingsType;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.esindexing.api.EsService.getIndexesByAliasAndIndexPattern;
import static org.gbif.pipelines.esindexing.api.EsService.swapIndexes;
import static org.gbif.pipelines.esindexing.api.EsService.updateIndexSettings;

/** Exposes a public API to perform operations in a ES instance. */
public class EsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EsHandler.class);

  static final String INDEX_SEPARATOR = "_";

  private EsHandler() {}

  /**
   * Creates an Index in the ES instance specified in the {@link EsConfig} received.
   *
   * <p>Both datasetId and attempt parameters are required. The index created will follow the
   * pattern "{datasetId}_{attempt}".
   *
   * @param config configuration of the ES instance.
   * @param datasetId dataset id.
   * @param attempt attempt of the dataset crawling.
   * @return name of the index created.
   */
  public static String createIndex(EsConfig config, String datasetId, int attempt) {
    final String idxName = createIndexName(datasetId, attempt);
    LOG.info("Creating index {}", idxName);

    try (EsClient esClient = EsClient.from(config)) {
      return EsService.createIndex(esClient, idxName, SettingsType.INDEXING);
    }
  }

  /**
   * Creates an Index in the ES instance specified in the {@link EsConfig} received.
   *
   * <p>Both datasetId and attempt parameters are required. The index created will follow the
   * pattern "{datasetId}_{attempt}".
   *
   * @param config configuration of the ES instance.
   * @param datasetId dataset id.
   * @param attempt attempt of the dataset crawling.
   * @param mappings path of the file with the mappings.
   * @return name of the index created.
   */
  public static String createIndex(EsConfig config, String datasetId, int attempt, Path mappings) {
    final String idxName = createIndexName(datasetId, attempt);
    LOG.info("Creating index {}", idxName);

    try (EsClient esClient = EsClient.from(config)) {
      return EsService.createIndex(esClient, idxName, SettingsType.INDEXING, mappings);
    }
  }

  /**
   * Creates an Index in the ES instance specified in the {@link EsConfig} received.
   *
   * <p>Both datasetId and attempt parameters are required. The index created will follow the
   * pattern "{datasetId}_{attempt}".
   *
   * @param config configuration of the ES instance.
   * @param datasetId dataset id.
   * @param attempt attempt of the dataset crawling.
   * @param mappings mappings as json string.
   * @return name of the index created.
   */
  public static String createIndex(
      EsConfig config, String datasetId, int attempt, String mappings) {
    final String idxName = createIndexName(datasetId, attempt);
    LOG.info("Creating index {}", idxName);

    try (EsClient esClient = EsClient.from(config)) {
      return EsService.createIndex(esClient, idxName, SettingsType.INDEXING, mappings);
    }
  }

  /**
   * Swaps an index in a alias.
   *
   * <p>The index received will be the only index associated to the alias after performing this
   * call. All the indexes that were associated to this alias before will be removed from the ES
   * instance.
   *
   * @param config configuration of the ES instance.
   * @param alias alias that will be modified.
   * @param index index to add to the alias that will become the only index of the alias.
   */
  public static void swapIndexInAlias(EsConfig config, String alias, String index) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(alias), "alias is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");

    LOG.info("Swapping index {} in alias {}", index, alias);

    // get dataset id
    String datasetId = getDatasetIdFromIndex(index);

    try (EsClient esClient = EsClient.from(config)) {
      // check if there are indexes to remove
      Set<String> idxToRemove =
          getIndexesByAliasAndIndexPattern(esClient, getDatasetIndexesPattern(datasetId), alias);

      // swap the indexes
      swapIndexes(esClient, alias, Collections.singleton(index), idxToRemove);

      // change index settings to search settings
      updateIndexSettings(esClient, index, SettingsType.SEARCH);
    }
  }

  /**
   * Counts the number of documents of an index.
   *
   * @param config configuration of the ES instance.
   * @param index index to count the elements from.
   * @return number of documents of the index.
   */
  public static long countIndexDocuments(EsConfig config, String index) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");

    LOG.info("Counting documents from index {}", index);

    try (EsClient esClient = EsClient.from(config)) {
      return EsService.countIndexDocuments(esClient, index);
    }
  }

  private static String getDatasetIndexesPattern(String datasetId) {
    return datasetId + INDEX_SEPARATOR + "*";
  }

  private static String getDatasetIdFromIndex(String index) {
    List<String> pieces = Splitter.on(INDEX_SEPARATOR).splitToList(index);

    if (pieces.size() != 2) {
      LOG.error("Index {} doesn't follow the pattern \"{datasetId}_{attempt}\"", index);
      throw new IllegalArgumentException(
          "index has to follow the pattern \"{datasetId}_{attempt}\"");
    }

    return pieces.get(0);
  }

  private static String createIndexName(String datasetId, int attempt) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetId), "dataset id is required");
    return datasetId + INDEX_SEPARATOR + attempt;
  }
}
