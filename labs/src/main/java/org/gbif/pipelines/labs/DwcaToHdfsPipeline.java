package org.gbif.pipelines.labs;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.TargetPath;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;
import org.gbif.pipelines.labs.functions.FunctionFactory;
import org.gbif.pipelines.labs.transform.TypeDescriptors;

import java.io.File;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline that transforms a Dwca file into an Avro file and writes it to HDFS. <p> This pipeline is intended to use
 * Dwca files that are in your local system. To use a dwca file from HDFS take a look at the pipelines in the
 * gbif-pipelines project.
 */
public class DwcaToHdfsPipeline {

  private static final String TMP_DIR_DWCA = "tmpDwca";

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToHdfsPipeline.class);

  /**
   * Main method.
   */
  public static void main(String[] args) {

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    Pipeline pipeline = Pipeline.create(options);

    String targetPath = TargetPath.fullPath(options.getDefaultTargetDirectory(), options.getDatasetId());

    LOG.info("Target path : {}", targetPath);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class, UntypedOccurrence.class);

    // temp dir for Dwca.
    String tmpDirDwca = new File(options.getInputFile()).getParentFile().getPath() + File.separator + TMP_DIR_DWCA;

    LOG.info("tmp dir Dwca : {}", tmpDirDwca);

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords =
        pipeline.apply("Read from Darwin Core Archive", DwCAIO.Read.withPaths(options.getInputFile(), tmpDirDwca));

    // TODO: Explore the generics as to whßy the coder registry does not find it and we need to set the coder explicitly
    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "Convert the objects into untyped DwC style records",
      MapElements.into(TypeDescriptors.untypedOccurrence()).via(FunctionFactory.untypedOccurrenceBuilder()))
      .setCoder(AvroCoder.of(UntypedOccurrence.class));

    verbatimRecords.apply("Write Avro files",
        AvroIO.write(UntypedOccurrence.class)
            .to(targetPath)
            .withTempDirectory(FileSystems.matchNewResource(options.getHdfsTempLocation(), true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());

  }

}
