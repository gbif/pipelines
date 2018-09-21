package org.gbif.pipelines.ingest.utils;

import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.service.EsConstants;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /** Connects to Elasticsearch instace and swaps an index and an alias */
  public static void swapIndex(EsIndexingPipelineOptions options) {
    EsConfig config = EsConfig.from(options.getEsHosts());

    String alias = options.getEsAlias();
    String index = options.getEsIndexName();

    EsIndex.swapIndexInAlias(config, alias, index);
    LOG.info("ES index {} added to alias {}", index, alias);

    EsIndex.refresh(config, index);
    long count = EsIndex.countDocuments(config, index);
    LOG.info("Index name - {}, Alias - {}, Number of records -  {}", index, alias, count);
  }
}
