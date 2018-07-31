package org.gbif.pipelines.minipipelines.dwca;

import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.esindexing.api.EsHandler;
import org.gbif.pipelines.esindexing.client.EsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Class that handles the creation and execution of a pipeline that works with Dwc-A files.
 *
 * <p>Depending on the steps of the pipeline the execution process will vary. The main difference is
 * when we are indexing in ES. In this case, we need to create the ES index before building the
 * pipeline in order to set that index into the indexing step. After executing the pipeline we also
 * have to swap the new index in the alias.
 *
 * <p>This class is intended to be used internally, so it should always be package-private.
 */
class DwcaPipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaPipelineRunner.class);

  private final DwcaPipelineOptions options;
  private final EsConfig esConfig;

  private DwcaPipelineRunner(DwcaPipelineOptions options) {
    this.options = options;
    esConfig = isEsIndexingIncludedInPipeline() ? EsConfig.from(options.getESHosts()) : null;
  }

  static DwcaPipelineRunner from(DwcaPipelineOptions options) {
    return new DwcaPipelineRunner(options);
  }

  void run() {
    // if ES indexing is included in the pipeline we first create the index
    createIndex().ifPresent(options::setESIndexName);

    // we build the pipeline. This has to be done after creating the index because we need to know
    // the name of the index where we'll index the records to.
    Pipeline pipeline = DwcaPipelineBuilder.buildPipeline(options);

    // we run the pipeline
    pipeline.run().waitUntilFinish();

    // finally we swap the index
    swapIndex();

    // we remove the tmp folder at shutdown
    removeTmp();
  }

  private Optional<String> createIndex() {
    if (isEsIndexingIncludedInPipeline()) {
      Path path = Paths.get(options.getESSchemaPath());
      String index =
          EsHandler.createIndex(esConfig, options.getDatasetId(), options.getAttempt(), path);
      LOG.info("ES index {} created", index);
      return Optional.of(index);
    }

    return Optional.empty();
  }

  private void swapIndex() {
    if (isEsIndexingIncludedInPipeline() && !Strings.isNullOrEmpty(options.getESAlias())) {
      EsHandler.swapIndexInAlias(esConfig, options.getESAlias(), options.getESIndexName());
      LOG.info("ES index {} added to alias {}", options.getESIndexName(), options.getESAlias());

      // log number of records indexed
      // TODO: find better way than refreshing?? other option is to wait 1s
      // refresh the index because the records are not available to search immediately.
      EsHandler.refreshIndex(esConfig, options.getESIndexName());
      long recordsIndexed = EsHandler.countIndexDocuments(esConfig, options.getESIndexName());
      LOG.info(
          "{} records indexed into the ES index {} in alias {}",
          recordsIndexed,
          options.getESIndexName(),
          options.getESAlias());
    }
  }

  private void removeTmp() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.debug("Dwca pipeline runner shutdown hook called");
                  File tmp = Paths.get(OutputWriter.getTempDir(options)).toFile();
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

  private boolean isEsIndexingIncludedInPipeline() {
    return options.getPipelineStep() == DwcaPipelineOptions.PipelineStep.INDEX_TO_ES;
  }
}
