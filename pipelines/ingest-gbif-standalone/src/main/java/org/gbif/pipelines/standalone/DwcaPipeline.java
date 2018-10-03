package org.gbif.pipelines.standalone;

import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToEsIndexPipeline;
import org.gbif.pipelines.ingest.pipelines.DwcaToInterpretedPipeline;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline;

/**
 * Entry point for running one of the four pipelines:
 *
 * <pre>
 *  1) From DwCA to ExtendedRecord *.avro file
 *  2) From DwCA to GBIF interpreted *.avro files
 *  3) From DwCA to Elasticsearch index
 *  4) From GBIF interpreted *.avro files to Elasticsearch index
 * </pre>
 *
 * Please read README.md
 */
public class DwcaPipeline {

  public static void main(String[] args) {

    // Create PipelineOptions
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);

    switch (options.getPipelineStep()) {
      case DWCA_TO_VERBATIM:
        DwcaToVerbatimPipeline.run(options);
        break;
      case DWCA_TO_INTERPRETED:
        DwcaToInterpretedPipeline.run(options);
        break;
      case DWCA_TO_ES_INDEX:
        DwcaToEsIndexPipeline.run(options);
        break;
      case INTERPRETED_TO_ES_INDEX:
        options.setTargetPath(options.getInputPath());
        InterpretedToEsIndexExtendedPipeline.run(options);
        break;
      case VERBATIM_TO_INTERPRETED:
        VerbatimToInterpretedPipeline.run(options);
        break;
      default:
        break;
    }
  }
}
