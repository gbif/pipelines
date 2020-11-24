package org.gbif.pipelines.estools;

import static org.gbif.pipelines.estools.service.EsConstants.Util.INDEX_SEPARATOR;
import static org.gbif.pipelines.estools.service.EsQueries.DELETE_BY_DATASET_QUERY;
import static org.gbif.pipelines.estools.service.EsService.getIndexesByAliasAndIndexPattern;
import static org.gbif.pipelines.estools.service.EsService.swapIndexes;
import static org.gbif.pipelines.estools.service.EsService.updateIndexSettings;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.model.DeleteByQueryTask;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsConstants.Searching;
import org.gbif.pipelines.estools.service.EsService;

/** Exposes a public API to perform operations in a ES instance. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsIndex {

  /**
   * Creates an ES index.
   *
   * @param config configuration of the ES instance.
   * @param indexParams parameters to create the index
   * @return name of the index
   */
  public static String createIndex(EsConfig config, IndexParams indexParams) {
    log.info("Creating index {}", indexParams.getIndexName());
    try (EsClient esClient = EsClient.from(config)) {
      return EsService.createIndex(esClient, indexParams);
    }
  }

  /**
   * Creates an ES index if it doesn't exist another index with the same name.
   *
   * @param config configuration of the ES instance.
   * @param indexParams parameters to create the index
   * @return name of the index. Empty if the index already existed.
   */
  public static Optional<String> createIndexIfNotExists(EsConfig config, IndexParams indexParams) {
    log.info("Creating index from params: {}", indexParams);
    try (EsClient esClient = EsClient.from(config)) {
      if (!EsService.existsIndex(esClient, indexParams.getIndexName())) {
        return Optional.of(EsService.createIndex(esClient, indexParams));
      }
    }

    return Optional.empty();
  }

  /**
   * Swaps an index in a aliases.
   *
   * <p>The index received will be the only index associated to the alias for the dataset after
   * performing this call. All the indexes that were associated to this alias before will be removed
   * from the ES instance.
   *
   * @param config configuration of the ES instance.
   * @param aliases aliases that will be modified.
   * @param index index to add to the aliases that will become the only index of the aliases for the
   *     dataset.
   */
  public static void swapIndexInAliases(EsConfig config, Set<String> aliases, String index) {
    Preconditions.checkArgument(aliases != null && !aliases.isEmpty(), "alias is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");
    swapIndexInAliases(
        config, aliases, index, Collections.emptySet(), Searching.getDefaultSearchSettings());
  }

  /**
   * Swaps indexes in alias. It adds the given index to the alias and removes all the indexes that
   * match the dataset pattern plus some extra indexes that can be passed as parameter.
   *
   * @param config configuration of the ES instance.
   * @param aliases aliases that will be modified.
   * @param index index to add to the aliases that will become the only index of the alias for the
   *     dataset.
   * @param extraIdxToRemove extra indexes to be removed from the aliases
   */
  public static void swapIndexInAliases(
      EsConfig config,
      Set<String> aliases,
      String index,
      Set<String> extraIdxToRemove,
      Map<String, String> settings) {
    Objects.requireNonNull(aliases, "aliases are required");

    Set<String> validAliases =
        aliases.stream().filter(alias -> !Strings.isNullOrEmpty(alias)).collect(Collectors.toSet());
    Preconditions.checkArgument(!validAliases.isEmpty(), "aliases are required");

    try (EsClient esClient = EsClient.from(config)) {

      Set<String> idxToAdd = new HashSet<>();
      Set<String> idxToRemove = new HashSet<>();

      // the index to add is optional
      Optional.ofNullable(index)
          .ifPresent(
              idx -> {
                idxToAdd.add(idx);

                // look for old indexes for this datasetId to remove them from the alias
                String datasetId = getDatasetIdFromIndex(idx);
                Optional.ofNullable(
                        getIndexesByAliasAndIndexPattern(
                            esClient, getDatasetIndexesPattern(datasetId), validAliases))
                    .ifPresent(idxToRemove::addAll);
              });

      // add extra indexes to remove
      Optional.ofNullable(extraIdxToRemove).ifPresent(idxToRemove::addAll);

      log.info(
          "Removing indexes {} and adding index {} in aliases {}",
          idxToRemove,
          index,
          validAliases);

      // swap the indexes
      swapIndexes(esClient, validAliases, idxToAdd, idxToRemove);

      Optional.ofNullable(index)
          .ifPresent(
              idx -> {
                // change index settings to search settings
                updateIndexSettings(esClient, idx, settings);

                EsService.refreshIndex(esClient, idx);
                long count = EsService.countIndexDocuments(esClient, idx);
                log.info("Index {} added to alias {} with {} records", idx, validAliases, count);
              });
    }
  }

  /**
   * Counts the number of documents of an index.
   *
   * @param config configuration of the ES instance.
   * @param index index to count the elements from.
   * @return number of documents of the index.
   */
  public static long countDocuments(EsConfig config, String index) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");
    log.info("Counting documents from index {}", index);
    try (EsClient esClient = EsClient.from(config)) {
      return EsService.countIndexDocuments(esClient, index);
    }
  }

  /**
   * Connects to Elasticsearch instance and deletes records in an index by datasetId and returns the
   * indexes where the dataset was present.
   *
   * @param config configuration of the ES instance.
   * @param aliases aliases where we look for records of the dataset
   * @param datasetKey dataset whose records we are looking for
   * @param indexesToDelete filters the indexes whose records we want to delete, so we can ignore
   *     some. E.g.: delete only from non-independent indexes.
   * @return datasets where we found records of this dataset
   */
  public static Set<String> deleteRecordsByDatasetId(
      EsConfig config,
      String[] aliases,
      String datasetKey,
      Predicate<String> indexesToDelete,
      int timeoutSec,
      int attempts) {

    try (EsClient esClient = EsClient.from(config)) {
      // find indexes where the dataset is present
      Set<String> existingDatasetIndexes =
          findDatasetIndexesInAliases(esClient, aliases, datasetKey);

      if (existingDatasetIndexes == null || existingDatasetIndexes.isEmpty()) {
        return Collections.emptySet();
      }

      // prepare parameters
      String query = String.format(DELETE_BY_DATASET_QUERY, datasetKey);

      // we only delete by query for the indexes specified
      String indexes =
          existingDatasetIndexes.stream().filter(indexesToDelete).collect(Collectors.joining(","));

      if (!Strings.isNullOrEmpty(indexes)) {
        log.info("Deleting records from ES indexes {} with query {}", indexes, query);
        deleteRecordsByQueryAndWaitTillCompletion(esClient, indexes, query, timeoutSec, attempts);
      }

      return existingDatasetIndexes;
    }
  }

  @SneakyThrows
  private static void deleteRecordsByQueryAndWaitTillCompletion(
      EsClient esClient, String index, String query, int timeoutSec, int attempts) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");
    String taskId = EsService.deleteRecordsByQuery(esClient, index, query);

    DeleteByQueryTask task = EsService.getDeletedByQueryTask(esClient, taskId);
    while (!task.isCompleted() && attempts-- > 0) {
      TimeUnit.SECONDS.sleep(timeoutSec);
      task = EsService.getDeletedByQueryTask(esClient, taskId);
    }

    log.info("{} records deleted from ES index {}", task.getRecordsDeleted(), index);
  }

  /**
   * Finds the indexes in an alias where a given dataset is present. This method checks that the
   * aliases exist before querying ES.
   *
   * @param config configuration of the ES instance.
   * @param aliases name of the alias to search in.
   * @param datasetKey key of the dataset we are looking for.
   */
  public static Set<String> findDatasetIndexesInAliases(
      EsConfig config, String[] aliases, String datasetKey) {
    try (EsClient esClient = EsClient.from(config)) {
      return findDatasetIndexesInAliases(esClient, aliases, datasetKey);
    }
  }

  private static Set<String> findDatasetIndexesInAliases(
      EsClient esClient, String[] aliases, String datasetKey) {
    Preconditions.checkArgument(aliases != null && aliases.length > 0, "aliases are required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetKey), "datasetKey is required");

    // we check if the aliases exist, otherwise ES throws an error.
    String existingAlias =
        Arrays.stream(aliases)
            .filter(alias -> EsService.existsIndex(esClient, alias))
            .collect(Collectors.joining(","));

    return EsService.findDatasetIndexesInAlias(esClient, existingAlias, datasetKey);
  }

  private static String getDatasetIndexesPattern(String datasetId) {
    return datasetId + INDEX_SEPARATOR + "*";
  }

  private static String getDatasetIdFromIndex(String index) {
    List<String> pieces = Arrays.asList(index.split(INDEX_SEPARATOR));

    if (pieces.size() < 2) {
      log.error("Index {} doesn't follow the pattern \"{datasetId}_{attempt}\"", index);
      throw new IllegalArgumentException(
          "index has to follow the pattern \"{datasetId}_{attempt}\"");
    }

    return pieces.get(0);
  }
}
