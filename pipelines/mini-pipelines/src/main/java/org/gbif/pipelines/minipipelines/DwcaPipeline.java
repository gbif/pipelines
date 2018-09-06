package org.gbif.pipelines.minipipelines;

import org.gbif.pipelines.base.options.PipelinesOptionsFactory;
import org.gbif.pipelines.base.pipelines.DwcaToAvroPipeline;
import org.gbif.pipelines.base.pipelines.IndexingWithCreationPipeline;

public class DwcaPipeline {

  public static void main(String[] args) {

    // Create PipelineOptions
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);

    switch (options.getPipelineStep()) {
      case DWCA_TO_AVRO:
        DwcaToAvroPipeline.create(options).run();
        break;
      case INTERPRET:
        DwcaInterpretationPipeline.create(options).run();
        break;
      case INDEX_TO_ES:
        DwcaIndexingPipeline.create(options).run();
        break;
      case AVRO_TO_INDEX:
        IndexingWithCreationPipeline.create(options).run();
        break;
      default:
        break;
    }
  }
}
