package org.gbif.pipelines.ingest.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.service.EsConstants;
import org.gbif.pipelines.estools.service.EsConstants.Field;
import org.gbif.pipelines.estools.service.EsConstants.Indexing;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.parsers.config.LockConfig;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.estools.service.EsQueries.DELETE_BY_DATASET_QUERY;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsIndexUtils {

  /** Connects to Elasticsearch instance and creates an index */
  public static void createIndex(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());
    Path mappingsPath = Paths.get(options.getEsSchemaPath());

    boolean independentIndex = options.getEsIndexName().startsWith(options.getDatasetId());

    Map<String, String> settings = new HashMap<>();
    settings.put(EsConstants.Field.INDEX_REFRESH_INTERVAL,
        independentIndex ? Indexing.REFRESH_INTERVAL : options.getIndexRefreshInterval());
    settings.put(EsConstants.Field.INDEX_NUMBER_SHARDS, options.getIndexNumberShards().toString());
    settings.put(EsConstants.Field.INDEX_NUMBER_REPLICAS,
        independentIndex ? Indexing.NUMBER_REPLICAS : options.getIndexNumberReplicas().toString());
    settings.put(Field.INDEX_ANALYSIS, Indexing.NORMALIZER);

    String idx;
    if (Strings.isNullOrEmpty(options.getEsIndexName())) {
      idx = EsIndex.create(config, options.getDatasetId(), options.getAttempt(), mappingsPath, settings);
    } else {
      idx = EsIndex.create(config, options.getEsIndexName(), mappingsPath, settings);
    }
    log.info("ES index {} created", idx);

    Optional.ofNullable(idx).ifPresent(options::setEsIndexName);
  }

  /** Connects to Elasticsearch instance and creates an index, if index doesn't exist */
  public static void createIndexIfNotExist(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());
    if (!EsIndex.indexExists(config, options.getEsIndexName())) {
      createIndex(options);
    }
  }

  /** Connects to Elasticsearch instance and swaps an index and an alias */
  public static void swapIndex(EsIndexingPipelineOptions options, LockConfig lockConfig) {
    EsConfig config = EsConfig.from(options.getEsHosts());

    Set<String> aliases = Sets.newHashSet(options.getEsAlias());
    String index = options.getEsIndexName();

    //Lock the index to avoid modifications and stop reads.
    SharedLockUtils.doInWriteLock(lockConfig, () -> {

      EsIndex.swapIndexInAliases(config, aliases, index);
      log.info("ES index {} added to alias {}", index, aliases);

      EsIndex.refresh(config, index);
    });
    long count = EsIndex.countDocuments(config, index);
    log.info("Index name - {}, Alias - {}, Number of records -  {}", index, aliases, count);
  }

  /** Connects to Elasticsearch instance and swaps an index and an alias, if alias exists */
  public static void swapIndexIfAliasExists(EsIndexingPipelineOptions options, LockConfig lockConfig) {
    String[] aliases = options.getEsAlias();
    if (aliases != null && aliases.length > 0) {
      swapIndex(options, lockConfig);
    }
  }

  /** Connects to Elasticsearch instance and swaps an index and an alias, if alias exists */
  public static void updateAlias(EsIndexingPipelineOptions options, Set<String> existingDatasetIndexes,
      LockConfig lockConfig) {
    Preconditions.checkArgument(options.getEsAlias() != null && options.getEsAlias().length > 0,
        "ES alias is required");
    Preconditions.checkArgument(existingDatasetIndexes != null, "The set with existing datasets cannot be null");

    EsConfig config = EsConfig.from(options.getEsHosts());

    String idxToAdd = options.getEsIndexName().startsWith(options.getDatasetId()) ? options.getEsIndexName() : null;

    Set<String> idxToRemove = existingDatasetIndexes.stream()
        .filter(i -> i.startsWith(options.getDatasetId()))
        .collect(Collectors.toSet());

    // we first check if there are indexes to swap to avoid unnecessary locks
    if (idxToAdd != null || !idxToRemove.isEmpty()) {
      Map<String, String> searchSettings = new HashMap<>();
      searchSettings.put(Field.INDEX_REFRESH_INTERVAL, options.getIndexRefreshInterval());
      searchSettings.put(Field.INDEX_NUMBER_REPLICAS, options.getIndexNumberReplicas().toString());

      SharedLockUtils.doInWriteLock(lockConfig, () -> {
        EsIndex.swapIndexInAliases(config, Sets.newHashSet(options.getEsAlias()), idxToAdd, idxToRemove,
            searchSettings);

        Optional.ofNullable(idxToAdd).ifPresent(idx -> EsIndex.refresh(config, idx));
      });
    }

    Optional.ofNullable(idxToAdd).ifPresent(idx -> {
      long count = EsIndex.countDocuments(config, idx);
      log.info("Index name - {}, Alias - {}, Number of records -  {}", idx, options.getEsAlias(), count);
    });

  }

  /** Connects to Elasticsearch instance and deletes records in an index by datasetId */
  public static void deleteRecordsByDatasetId(EsIndexingPipelineOptions options, Set<String> existingDatasetIndexes) {
    if (existingDatasetIndexes == null || existingDatasetIndexes.isEmpty()) {
      return;
    }

    // prepare parameters
    EsConfig config = EsConfig.from(options.getEsHosts());
    String query = String.format(DELETE_BY_DATASET_QUERY, options.getDatasetId());

    // we only delete by query for indexes that contain multiple datasets
    String indexes = existingDatasetIndexes.stream()
        .filter(i -> !i.startsWith(options.getDatasetId()))
        .collect(Collectors.joining(","));

    if (Strings.isNullOrEmpty(indexes)) {
      return;
    }

    log.info("ES indexes {} delete records by query {}", indexes, query);
    EsIndex.deleteRecordsByQueryAndWaitTillCompletion(config, indexes, query);
  }

  public static Set<String> findDatasetIndexesInAlias(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());
    return EsIndex.findDatasetIndexesInAliases(config, options.getEsAlias(), options.getDatasetId());
  }
}
