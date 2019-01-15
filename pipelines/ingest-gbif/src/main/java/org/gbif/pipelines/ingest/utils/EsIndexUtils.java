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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class EsIndexUtils {

  private static final Logger LOG = LoggerFactory.getLogger(EsIndexUtils.class);

  private EsIndexUtils() {}

  /** Connects to Elasticsearch instace and creates an index */
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
    LOG.info("ES index {} created", idx);

    Optional.ofNullable(idx).ifPresent(options::setEsIndexName);
  }

  /** Connects to Elasticsearch instace and creates an index, if index doesn't exist */
  public static void createIndexIfNotExist(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());
    if (!EsIndex.indexExists(config, options.getEsIndexName())) {
      createIndex(options);
    }
  }

  /** Connects to Elasticsearch instace and swaps an index and an alias */
  public static void swapIndex(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());

    String[] aliases = options.getEsAlias();
    String index = options.getEsIndexName();

    EsIndex.swapIndexInAliases(config, aliases, index);
    LOG.info("ES index {} added to alias {}", index, aliases);

    EsIndex.refresh(config, index);
    long count = EsIndex.countDocuments(config, index);
    LOG.info("Index name - {}, Alias - {}, Number of records -  {}", index, aliases, count);
  }

  /** Connects to Elasticsearch instace and swaps an index and an alias, if alias exists */
  public static void swapIndexIfAliasExists(EsIndexingPipelineOptions options) {
    String[] aliases = options.getEsAlias();
    if (aliases != null && aliases.length > 0) {
      swapIndex(options);
    }
  }

  /**
   * TODO:DOC
   */
  public static void deleteRecordsByDatasetId(EsIndexingPipelineOptions options) {
    String[] aliases = options.getEsAlias();
    if (aliases != null && aliases.length > 0) {
      return;
    }

    EsConfig config = EsConfig.from(options.getEsHosts());
    String index = options.getEsIndexName();
    String query = "{\"query\":{\"match\":{\"datasetKey\":\"" + options.getDatasetId() + "\"}}}";

    LOG.info("ES index {} delete records by query {}", index, query);
    EsIndex.deleteRecordsByQuery(config, index, query);
  }
}
