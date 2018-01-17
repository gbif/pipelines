package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.common.beam.BeamFunctions;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.config.HdfsExporterOptions;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.demo.utils.PipelineUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.io.File;
import java.util.Optional;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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

  private HdfsExporterOptions options;

  public DwcaToHdfsTestingPipeline(HdfsExporterOptions options) {
    this.options = options;
  }

  public void createAndRunPipeline() {
    Optional.ofNullable(options).orElseThrow(() -> new IllegalArgumentException("Pipeline options cannot be null"));

    String targetPath = PipelineUtils.targetPath(options);

    LOG.info("Target path : {}", targetPath);

    Pipeline pipeline = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class, UntypedOccurrence.class);

    // temp dir for Dwca
    String tmpDirDwca = new File(options.getInputFile()).getParentFile().getPath() + File.separator + "tmpDwca";

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords = pipeline.apply("Read from Darwin Core Archive",
                                                            DwCAIO.Read.withPaths(options.getInputFile(),
                                                                                  options.getHdfsTempLocation()));

    // Convert the ExtendedRecord into an UntypedOccurrence record
    DoFn<ExtendedRecord, UntypedOccurrence> fn = BeamFunctions.beamify(FunctionFactory.untypedOccurrenceBuilder());

    // TODO: Explore the generics as to whßy the coder registry does not find it and we need to set the coder explicitly
    PCollection<UntypedOccurrence> verbatimRecords =
      rawRecords.apply("Convert the objects into untyped DwC style records", ParDo.of(fn))
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
