package org.gbif.pipelines.minipipelines.dwca;

import org.apache.beam.sdk.Pipeline;
import org.gbif.pipelines.esindexing.api.EsHandler;
import org.gbif.pipelines.esindexing.client.EsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;

/**
 * Class that handles the creation and execution of a pipeline that works with Dwc-A files.
 * Depending on the steps of the pipeline the execution process will vary.
 *
 * <p>This class is intended to be used internally, so it should always be package-private.
 */
class DwcaPipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaPipelineRunner.class);

  private final DwcaMiniPipelineOptions options;
  private String idxCreated;
  private EsConfig esConfig;

  private DwcaPipelineRunner(DwcaMiniPipelineOptions options) {
    this.options = options;
  }

  static DwcaPipelineRunner from(DwcaMiniPipelineOptions options) {
    return new DwcaPipelineRunner(options);
  }

  void run() {
    // if ES indexing is included in the pipeline we first create the index
    createIndex();

    // we build the pipeline. This has to be done after creating the index because we need to know
    // the name of the index where we'll index the records to.
    Pipeline pipeline = DwcaPipelineBuilder.buildPipeline(options);

    // we run the pipeline
    pipeline.run().waitUntilFinish();

    // finally we swap the index
    swapIndex();

    // we remove the tmp folder at shutdown
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Dwca pipeline runner shutdown hook called");
                  File tmp =
                      Paths.get(DwcaPipelineBuilder.OutputWriter.getTempDir(options)).toFile();
                  if (tmp.delete()) {
                    LOG.info("temp directory {} deleted", tmp.getPath());
                  } else {
                    LOG.warn("Could not delete temp directory {}", tmp.getPath());
                  }
                }));
  }

  private void createIndex() {
    if (isEsIncludedInPipeline()) {
      esConfig = EsConfig.from(options.getESAddresses());
      idxCreated = EsHandler.createIndex(esConfig, options.getDatasetId(), options.getAttempt());
      options.setESIndexName(idxCreated);
      LOG.info("ES index {} created", idxCreated);
    }
  }

  private void swapIndex() {
    if (isEsIncludedInPipeline()) {
      EsHandler.swapIndexInAlias(esConfig, options.getESAlias(), idxCreated);
      LOG.info("ES index {} added to alias {}", idxCreated, options.getESAlias());

      // log number of records indexed
      // TODO: find better way than refreshing??
      // refresh the index because the records are not available to search immediately.
      EsHandler.refreshIndex(esConfig, idxCreated);
      long recordsIndexed = EsHandler.countIndexDocuments(esConfig, idxCreated);
      LOG.info(
          "{} records indexed into the ES index {} in alias {}",
          recordsIndexed,
          idxCreated,
          options.getESAlias());
    }
  }

  private boolean isEsIncludedInPipeline() {
    return options.getPipelineStep() == DwcaMiniPipelineOptions.PipelineStep.INDEX_TO_ES;
  }
}
