package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.DataPipelineOptionsFactory;
import org.gbif.pipelines.base.options.EsProcessingPipelineOptions;
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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.base.utils.FsUtils.buildPathString;

import static com.google.common.base.Strings.isNullOrEmpty;

/** TODO: DOC */
public class IndexingWithCreationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingWithCreationPipeline.class);

  private final EsProcessingPipelineOptions options;
  private final EsConfig esConfig;

  private IndexingWithCreationPipeline(EsProcessingPipelineOptions options) {
    this.options = options;
    this.esConfig = EsConfig.from(options.getESHosts());
  }

  public static IndexingWithCreationPipeline create(EsProcessingPipelineOptions options) {
    return new IndexingWithCreationPipeline(options);
  }

  /** TODO: DOC */
  public static void main(String[] args) {
    EsProcessingPipelineOptions options = DataPipelineOptionsFactory.createForEs(args);
    IndexingWithCreationPipeline.create(options).run();
  }

  /** TODO: DOC */
  public void run() {

    createIndex();

    IndexingPipeline.create(options).run();

    swapIndex();

    removeTmpDirecrory();
  }

  /** If ES indexing is included in the pipeline we first create the index */
  private void createIndex() {
    options.getESIndexName();
    Path path = Paths.get(options.getESSchemaPath());

    Map<String, String> map = new HashMap<>();
    map.put(EsConstants.Field.INDEX_REFRESH_INTERVAL, options.getIndexRefreshInterval());
    map.put(EsConstants.Field.INDEX_NUMBER_SHARDS, options.getIndexNumberShards().toString());
    map.put(EsConstants.Field.INDEX_NUMBER_REPLICAS, options.getIndexNumberReplicas().toString());

    String idx = EsIndex.create(esConfig, options.getDatasetId(), options.getAttempt(), path, map);
    LOG.info("ES index {} created", idx);

    Optional.of(idx).ifPresent(options::setESIndexName);
  }

  /** TODO: DOC */
  private void swapIndex() {
    String alias = options.getESAlias();
    String index = options.getESIndexName();

    EsIndex.swapIndexInAlias(esConfig, alias, index);
    LOG.info("ES index {} added to alias {}", index, alias);

    EsIndex.refresh(esConfig, index);
    long count = EsIndex.countDocuments(esConfig, index);
    LOG.info("Index name - {}, Alias - {}, Number of records -  {}", index, alias, count);
  }

  /** TODO: DOC */
  private void removeTmpDirecrory() {
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
