package org.gbif.pipelines.ingest.java;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;

import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToEsIndexExtendedPipeline {

  public static void main(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    InterpretedToEsIndexExtendedPipeline.run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline.run(options, () -> InterpretedToEsIndexPipeline.run(options));

    FsUtils.removeTmpDirectory(options);
    log.info("Finished main indexing pipeline");
  }
}
