package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.common.beam.BeamFunctions;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.demo.utils.PipelineUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline that transforms a Dwca file into an Avro file and writes it to HDFS.
 *
 * Source path is hard coded for demo purposes.
 */
public class DwcaToHdfsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToHdfsPipeline.class);

  /**
   * Main method.
   *
   * @param args beam arguments + -targetPath to specify the path where the files should be written.
   */
  public static void main(String[] args) {

    String targetPath = PipelineUtils.getTargetPath(args);

    LOG.info("Target path : {}", targetPath);

    Configuration config = new Configuration();
    Pipeline p = PipelineUtils.newHadoopPipeline(args, config);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p, ExtendedRecord.class, UntypedOccurrence.class);

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords = p.apply("Read from Darwin Core Archive",
                                                     DwCAIO.Read.withPaths("/home/mlopez/tests/dwca.zip",
                                                                           "/home/mlopez/tmp"));

    // Convert the ExtendedRecord into an UntypedOccurrence record
    DoFn<ExtendedRecord, UntypedOccurrence> fn = BeamFunctions.beamify(FunctionFactory.untypedOccurrenceBuilder());

    // TODO: Explore the generics as to whßy the coder registry does not find it and we need to set the coder explicitly
    PCollection<UntypedOccurrence> verbatimRecords =
      rawRecords.apply("Convert the objects into untyped DwC style records", ParDo.of(fn))
        .setCoder(AvroCoder.of(UntypedOccurrence.class));

    verbatimRecords.apply("Write Avro files",
                          AvroIO.write(UntypedOccurrence.class)
                            .to(targetPath)
                            .withTempDirectory(FileSystems.matchNewResource(PipelineUtils.tmpPath(config), true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());

  }

}
