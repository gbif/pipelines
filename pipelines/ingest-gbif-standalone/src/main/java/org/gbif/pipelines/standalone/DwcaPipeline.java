package org.gbif.pipelines.standalone;

import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToEsIndexPipeline;
import org.gbif.pipelines.ingest.pipelines.DwcaToInterpretedPipeline;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexAmpPipeline;
import org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.pipelines.ingest.pipelines.InterpretedToHdfsViewPipeline;
import org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedAmpPipeline;
import org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline;
import org.gbif.pipelines.ingest.pipelines.XmlToEsIndexPipeline;
import org.gbif.pipelines.ingest.pipelines.XmlToInterpretedPipeline;
import org.gbif.pipelines.ingest.pipelines.XmlToVerbatimPipeline;

/**
 * The entry point for running one of the standalone pipelines
 */
public class DwcaPipeline {

  public static void main(String[] args) {

    // Create PipelineOptions
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);

    switch (options.getPipelineStep()) {
      // From DwCA to ExtendedRecord *.avro file
      case DWCA_TO_VERBATIM:
        DwcaToVerbatimPipeline.run(options);
        break;
      // From DwCA to GBIF interpreted *.avro files
      case DWCA_TO_INTERPRETED:
        DwcaToInterpretedPipeline.run(options);
        break;
      // From DwCA to Elasticsearch index
      case DWCA_TO_ES_INDEX:
        DwcaToEsIndexPipeline.run(options);
        break;
      // From XML to ExtendedRecord *.avro file
      case XML_TO_VERBATIM:
        XmlToVerbatimPipeline.run(options);
        break;
      // From XML to GBIF interpreted *.avro files
      case XML_TO_INTERPRETED:
        XmlToInterpretedPipeline.run(options);
        break;
      // From XML to Elasticsearch index
      case XML_TO_ES_INDEX:
        XmlToEsIndexPipeline.run(options);
        break;
      // From GBIF interpreted *.avro files to Elasticsearch index
      case INTERPRETED_TO_ES_INDEX:
        options.setTargetPath(options.getInputPath());
        PipelinesOptionsFactory.registerHdfs(options);
        InterpretedToEsIndexExtendedPipeline.run(options);
        break;
      // From GBIF interpreted *.avro files into HDFS view avro files
      case INTERPRETED_TO_HDFS:
        PipelinesOptionsFactory.registerHdfs(options);
        InterpretedToHdfsViewPipeline.run(options);
        break;
      // From ExtendedRecord *.avro file to GBIF interpreted *.avro files
      case VERBATIM_TO_INTERPRETED:
        PipelinesOptionsFactory.registerHdfs(options);
        VerbatimToInterpretedPipeline.run(options);
        break;
      // From interpreted amplification extension *.avro files to appended Elasticsearch index
      case INTERPRETED_TO_ES_INDEX_AMP:
        options.setTargetPath(options.getInputPath());
        PipelinesOptionsFactory.registerHdfs(options);
        InterpretedToEsIndexAmpPipeline.run(options);
        break;
      // From ExtendedRecord *.avro file to interpreted amplification extension *.avro files
      case VERBATIM_TO_INTERPRETED_AMP:
        PipelinesOptionsFactory.registerHdfs(options);
        VerbatimToInterpretedAmpPipeline.run(options);
        break;
      default:
        break;
    }
  }
}
