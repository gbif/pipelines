package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.TargetPath;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.core.functions.descriptor.CustomTypeDescriptors;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.io.File;
import java.util.Objects;

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
 * Pipeline that writes an Avro file to HDFS.
 * <p>
 * Created to run junit tests against it.
 */
public class DwcaToHdfsTestingPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToHdfsTestingPipeline.class);

  private DataProcessingPipelineOptions options;

  public DwcaToHdfsTestingPipeline(DataProcessingPipelineOptions options) {
    this.options = options;
  }

  public void createAndRunPipeline() {
    Objects.requireNonNull(options, "Pipeline options cannot be null");

    String targetPath = TargetPath.fullPath(options.getDefaultTargetDirectory(), options.getDatasetId());

    LOG.info("Target path : {}", targetPath);

    Pipeline pipeline = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class, UntypedOccurrence.class);

    // temp dir for Dwca
    String tmpDirDwca = new File(options.getInputFile()).getParentFile().getPath() + File.separator + "tmpDwca";

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords =
      pipeline.apply("Read from Darwin Core Archive", DwCAIO.Read.withPaths(options.getInputFile(), tmpDirDwca));

        // TODO: Explore the generics as to whßy the coder registry does not find it and we need to set the coder explicitly
    PCollection<UntypedOccurrence> verbatimRecords =
      rawRecords.apply("Convert the objects into untyped DwC style records",
                       MapElements.into(CustomTypeDescriptors.untypedOccurrencies())
                                  .via(FunctionFactory.untypedOccurrenceBuilder()::apply))
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
