package org.gbif.pipelines.base.utils;

import org.gbif.pipelines.base.options.IndexingPipelineOptions;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.service.EsConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.base.utils.FsUtils.buildPathString;

import static com.google.common.base.Strings.isNullOrEmpty;

public class IndexingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingUtils.class);

  private IndexingUtils() {}

  /** If ES indexing is included in the pipeline we first create the index */
  public static void createIndex(IndexingPipelineOptions options, EsConfig config) {
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

    Optional.of(idx).ifPresent(options::setEsIndexName);
  }

  /** TODO: DOC */
  public static void swapIndex(IndexingPipelineOptions options, EsConfig config) {
    String alias = options.getEsAlias();
    String index = options.getEsIndexName();

    EsIndex.swapIndexInAlias(config, alias, index);
    LOG.info("ES index {} added to alias {}", index, alias);

    EsIndex.refresh(config, index);
    long count = EsIndex.countDocuments(config, index);
    LOG.info("Index name - {}, Alias - {}, Number of records -  {}", index, alias, count);
  }

  /** TODO: DOC */
  public static void removeTmpDirecrory(IndexingPipelineOptions options) {
    Runnable runnable =
        () -> {
          String l = options.getTempLocation();
          String t = isNullOrEmpty(l) ? buildPathString(options.getTargetPath(), "tmp") : l;
          File tmp = Paths.get(t).toFile();
          if (tmp.exists()) {
            try {
              FileUtils.deleteDirectory(tmp);
              LOG.info("temp directory {} deleted", tmp.getPath());
            } catch (IOException e) {
              LOG.warn("Could not delete temp directory {}", tmp.getPath());
            }
          }
        };

    Runtime.getRuntime().addShutdownHook(new Thread(runnable));
  }
}
