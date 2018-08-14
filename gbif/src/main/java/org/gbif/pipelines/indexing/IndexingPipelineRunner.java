package org.gbif.pipelines.indexing;

import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.esindexing.api.EsHandler;
import org.gbif.pipelines.esindexing.client.EsConfig;
import org.gbif.pipelines.esindexing.common.EsConstants;
import org.gbif.pipelines.utils.FsUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingPipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingPipelineRunner.class);

  private final EsProcessingPipelineOptions options;
  private final EsConfig esConfig;

  private IndexingPipelineRunner(EsProcessingPipelineOptions options) {
    this.options = options;
    esConfig = EsConfig.from(options.getESHosts());
  }

  // visible to be able to use in the mini-pipelines
  public static IndexingPipelineRunner from(EsProcessingPipelineOptions options) {
    return new IndexingPipelineRunner(options);
  }

  // visible to be able to use in the mini-pipelines
  public void run() {
    // if ES indexing is included in the pipeline we first create the index
    createIndex().ifPresent(options::setESIndexName);

    // we build the pipeline. This has to be done after creating the index because we need to know
    // the name of the index where we'll index the records to.
    Pipeline pipeline = IndexingPipelineBuilder.buildPipeline(options);

    // we run the pipeline
    pipeline.run().waitUntilFinish();

    // finally we swap the index
    swapIndex();

    // we remove the tmp folder at shutdown
    removeTmp();
  }

  private Optional<String> createIndex() {
    Path path = Paths.get(options.getESSchemaPath());
    String index =
        EsHandler.createIndex(
            esConfig, options.getDatasetId(), options.getAttempt(), path, createEsOptions());
    LOG.info("ES index {} created", index);
    return Optional.of(index);
  }

  private void swapIndex() {
    EsHandler.swapIndexInAlias(esConfig, options.getDatasetId(), options.getESIndexName());
    LOG.info("ES index {} added to alias {}", options.getESIndexName(), options.getDatasetId());

    // log number of records indexed
    // TODO: find better way than refreshing?? other option is to wait 1s
    // refresh the index because the records are not available to search immediately.
    EsHandler.refreshIndex(esConfig, options.getESIndexName());
    long recordsIndexed = EsHandler.countIndexDocuments(esConfig, options.getESIndexName());
    LOG.info(
        "{} records indexed into the ES index {} in alias {}",
        recordsIndexed,
        options.getESIndexName(),
        options.getDatasetId());
  }

  private void removeTmp() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.debug("Dwca pipeline runner shutdown hook called");
                  File tmp = Paths.get(getTempDir(options)).toFile();
                  if (tmp.exists()) {
                    try {
                      FileUtils.deleteDirectory(tmp);
                      LOG.info("temp directory {} deleted", tmp.getPath());
                    } catch (IOException e) {
                      LOG.warn("Could not delete temp directory {}", tmp.getPath());
                    }
                  }
                }));
  }

  private static String getTempDir(EsProcessingPipelineOptions options) {
    return Strings.isNullOrEmpty(options.getTempLocation())
        ? FsUtils.buildPathString(options.getTargetPath(), "tmp")
        : options.getTempLocation();
  }

  private Map<String, String> createEsOptions() {
    Map<String, String> map = new HashMap<>();
    map.put(EsConstants.Field.INDEX_REFRESH_INTERVAL, options.getIndexRefreshInterval());
    map.put(EsConstants.Field.INDEX_NUMBER_SHARDS, options.getIndexNumberShards().toString());
    map.put(EsConstants.Field.INDEX_NUMBER_REPLICAS, options.getIndexNumberReplicas().toString());
    return map;
  }
}
