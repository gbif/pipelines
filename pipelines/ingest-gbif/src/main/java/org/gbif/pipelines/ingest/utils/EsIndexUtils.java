package org.gbif.pipelines.ingest.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.service.EsConstants;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.parsers.config.LockConfig;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsIndexUtils {

  /** Connects to Elasticsearch instance and creates an index */
  public static void createIndex(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());
    Path path = Paths.get(options.getEsSchemaPath());

    Map<String, String> map = new HashMap<>();
    map.put(EsConstants.Field.INDEX_REFRESH_INTERVAL, options.getIndexRefreshInterval());
    map.put(EsConstants.Field.INDEX_NUMBER_SHARDS, options.getIndexNumberShards().toString());
    map.put(EsConstants.Field.INDEX_NUMBER_REPLICAS, options.getIndexNumberReplicas().toString());

    String idx;
    if (Strings.isNullOrEmpty(options.getEsIndexName())) {
      idx = EsIndex.create(config, options.getDatasetId(), options.getAttempt(), path, map);
    } else {
      idx = EsIndex.create(config, options.getEsIndexName(), path, map);
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

    String[] aliases = options.getEsAlias();
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

  /** Connects to Elasticsearch instance and deletes records in an index by datasetId */
  public static void deleteRecordsByDatasetId(EsIndexingPipelineOptions options) {
    String[] aliases = options.getEsAlias();
    if (aliases != null && aliases.length > 0) {
      return;
    }

    EsConfig config = EsConfig.from(options.getEsHosts());
    String index = options.getEsIndexName();
    String query = "{\"query\":{\"match\":{\"datasetKey\":\"" + options.getDatasetId() + "\"}}}";

    log.info("ES index {} delete records by query {}", index, query);
    EsIndex.deleteRecordsByQuery(config, index, query);
  }
}
