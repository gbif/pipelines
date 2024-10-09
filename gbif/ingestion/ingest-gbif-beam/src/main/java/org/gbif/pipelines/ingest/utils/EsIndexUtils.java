package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.estools.service.EsService.swapIndexes;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.core.config.model.LockConfig;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsConstants.Field;
import org.gbif.pipelines.estools.service.EsConstants.Indexing;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.wrangler.lock.Mutex;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsIndexUtils {

  /** Connects to Elasticsearch instance and creates an index. */
  public static void createIndex(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());

    String idx = EsIndex.createIndex(config, createIndexParams(options));
    log.info("ES index {} created", idx);

    Optional.ofNullable(idx).ifPresent(options::setEsIndexName);
  }

  /** Connects to Elasticsearch instance and creates an index, if index doesn't exist */
  public static void createIndexAndAliasForDefault(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());
    IndexParams params = createIndexParams(options);

    log.info("Creating index from params: {}", params);
    try (EsClient esClient = EsClient.from(config)) {
      if (!EsService.existsIndex(esClient, params.getIndexName())) {
        EsService.createIndex(esClient, params);
        addIndexAliasForDefault(esClient, options);
      }
    }
  }

  /** Add alias to index if the index is default/regular (it will contain many datasets) */
  private static void addIndexAliasForDefault(
      EsClient esClient, EsIndexingPipelineOptions options) {
    String index = options.getEsIndexName();
    Objects.requireNonNull(index, "index are required");
    if (!index.startsWith(options.getDatasetId())) {
      Set<String> aliases = new HashSet<>(Arrays.asList(options.getEsAlias()));

      Objects.requireNonNull(aliases, "aliases are required");

      Set<String> validAliases =
          aliases.stream()
              .filter(alias -> !Strings.isNullOrEmpty(alias))
              .collect(Collectors.toSet());
      Preconditions.checkArgument(!validAliases.isEmpty(), "aliases are required");

      swapIndexes(esClient, validAliases, Collections.singleton(index), Collections.emptySet());
    }
  }

  private static IndexParams createIndexParams(EsIndexingPipelineOptions options) {
    Path mappingsPath = Paths.get(options.getEsSchemaPath());

    boolean independentIndex = options.getEsIndexName().startsWith(options.getDatasetId());

    Map<String, String> settings = new HashMap<>(6);
    settings.put(
        Field.INDEX_REFRESH_INTERVAL,
        independentIndex ? Indexing.REFRESH_INTERVAL : options.getIndexRefreshInterval());
    settings.put(Field.INDEX_NUMBER_SHARDS, options.getIndexNumberShards().toString());
    settings.put(
        Field.INDEX_NUMBER_REPLICAS,
        independentIndex ? Indexing.NUMBER_REPLICAS : options.getIndexNumberReplicas().toString());
    settings.put(Field.INDEX_ANALYSIS, Indexing.ANALYSIS);
    settings.put(Field.INDEX_MAX_RESULT_WINDOW, options.getIndexMaxResultWindow().toString());
    settings.put(Field.INDEX_UNASSIGNED_NODE_DELAY, options.getUnassignedNodeDelay());

    if (options.getUseSlowlog()) {
      settings.put(
          Field.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN,
          options.getIndexSearchSlowlogThresholdQueryWarn());
      settings.put(
          Field.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO,
          options.getIndexSearchSlowlogThresholdQueryInfo());
      settings.put(
          Field.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN,
          options.getIndexSearchSlowlogThresholdFetchWarn());
      settings.put(
          Field.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO,
          options.getIndexSearchSlowlogThresholdFetchInfo());
      settings.put(Field.INDEX_SEARCH_SLOWLOG_LEVEL, options.getIndexSearchSlowlogLevel());
    }

    return IndexParams.builder()
        .indexName(options.getEsIndexName())
        .datasetKey(options.getDatasetId())
        .attempt(options.getAttempt())
        .pathMappings(mappingsPath)
        .settings(settings)
        .build();
  }

  /** Connects to Elasticsearch instance and swaps an index and an alias. */
  public static void swapIndex(EsIndexingPipelineOptions options, LockConfig lockConfig) {
    EsConfig config = EsConfig.from(options.getEsHosts());

    Set<String> aliases = new HashSet<>(Arrays.asList(options.getEsAlias()));
    String index = options.getEsIndexName();

    // Lock the index to avoid modifications and stop reads.
    SharedLockUtils.doInWriteLock(
        lockConfig,
        () -> {
          EsIndex.swapIndexInAliases(config, aliases, index);
          log.info("ES index {} added to alias {}", index, aliases);
        });
  }

  /** Connects to Elasticsearch instance and swaps an index and an alias, if alias exists. */
  public static void swapIndexIfAliasExists(
      EsIndexingPipelineOptions options, LockConfig lockConfig) {
    String[] aliases = options.getEsAlias();
    if (aliases != null && aliases.length > 0) {
      swapIndex(options, lockConfig);
    }
  }

  /** Connects to Elasticsearch instance and swaps an index and an alias, if alias exists. */
  public static void updateAlias(
      EsIndexingPipelineOptions options,
      Set<String> existingDatasetIndexes,
      LockConfig lockConfig) {
    Preconditions.checkArgument(
        options.getEsAlias() != null && options.getEsAlias().length > 0, "ES alias is required");
    Preconditions.checkArgument(
        existingDatasetIndexes != null, "The set with existing datasets cannot be null");

    EsConfig config = EsConfig.from(options.getEsHosts());

    String idxToAdd =
        options.getEsIndexName().startsWith(options.getDatasetId())
            ? options.getEsIndexName()
            : null;

    Set<String> idxToRemove =
        existingDatasetIndexes.stream()
            .filter(i -> i.startsWith(options.getDatasetId()))
            .collect(Collectors.toSet());

    // we first check if there are indexes to swap to avoid unnecessary locks
    if (idxToAdd != null || !idxToRemove.isEmpty()) {
      Map<String, String> searchSettings = new HashMap<>(2);
      searchSettings.put(Field.INDEX_REFRESH_INTERVAL, options.getIndexRefreshInterval());
      searchSettings.put(Field.INDEX_NUMBER_REPLICAS, options.getIndexNumberReplicas().toString());

      Mutex.Action action =
          () ->
              EsIndex.swapIndexInAliases(
                  config,
                  new HashSet<>(Arrays.asList(options.getEsAlias())),
                  idxToAdd,
                  idxToRemove,
                  searchSettings);
      if (lockConfig != null) {
        SharedLockUtils.doInWriteLock(lockConfig, action);
      } else {
        action.execute();
      }
    }
  }

  /**
   * Connects to Elasticsearch instance and deletes records in an index by datasetId and returns the
   * indexes where the dataset was present
   */
  public static Set<String> deleteRecordsByDatasetId(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());
    return EsIndex.deleteRecordsByDatasetId(
        config,
        options.getEsAlias(),
        options.getDatasetId(),
        idxName -> !idxName.startsWith(options.getDatasetId()),
        options.getSearchQueryTimeoutSec(),
        options.getSearchQueryAttempts());
  }

  /**
   * Connects to Elasticsearch instance and refreshes index to make queries work without waiting for
   * an update timeout
   */
  public static void refreshIndex(EsIndexingPipelineOptions options) {
    try (EsClient esClient = EsClient.from(EsConfig.from(options.getEsHosts()))) {
      EsService.refreshIndex(esClient, options.getEsIndexName());
    }
  }

  /** Connects to Elasticsearch instance and get documents count by dataset key */
  public static long getDocumentsCountByDatasetKey(EsIndexingPipelineOptions options) {
    try (EsClient esClient = EsClient.from(EsConfig.from(options.getEsHosts()))) {
      return EsService.countIndexDocumentsByDatasetKey(
          esClient, options.getEsIndexName(), options.getDatasetId());
    }
  }
}
