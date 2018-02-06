package org.gbif.pipelines.demo;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;

/**
 * A simple local demonstration illustrating taking an avro file and splitting it into an avro file per genus.
 */
public class MultiAvroOutDemo {

  private static final Logger LOG = LoggerFactory.getLogger(MultiAvroOutDemo.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    //options.setRunner(DirectRunner.class); // forced for this demo
    Pipeline p = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p, ExtendedRecord.class, UntypedOccurrence.class);

    PCollection<UntypedOccurrence> verbatimRecords =
      p.apply("Read Avro", AvroIO.read(UntypedOccurrence.class).from("demo/output/data*"));

    verbatimRecords.apply("Write file per Genus",
                          AvroIO.write(UntypedOccurrence.class)
                            .to("demo/output-split/data*") // prefix, is required but overwritten
                            .to(new GenusDynamicAvroDestinations(FileSystems.matchNewResource("demo/output-split/data*",
                                                                                              true))));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  /**
   * The provider of dynamic file names based on the genus field.
   */
  static class GenusDynamicAvroDestinations
    extends DynamicAvroDestinations<UntypedOccurrence, String, UntypedOccurrence> {

    ResourceId baseDir;

    GenusDynamicAvroDestinations(ResourceId baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    public Schema getSchema(String destination) {
      return UntypedOccurrence.SCHEMA$;
    }

    @Override
    public UntypedOccurrence formatRecord(UntypedOccurrence record) {
      return record;
    }

    @Override
    public String getDestination(UntypedOccurrence element) {
      return String.valueOf(element.getGenus());
    }

    @Override
    public String getDefaultDestination() {
      return ""; // ignored
    }

    @Override
    public FileBasedSink.FilenamePolicy getFilenamePolicy(String genus) {
      return DefaultFilenamePolicy.fromStandardParameters(ValueProvider.StaticValueProvider.of(baseDir.resolve(genus,
                                                                                                               RESOLVE_FILE)),
                                                          ShardNameTemplate.INDEX_OF_MAX,
                                                          ".avro",
                                                          false);
    }
  }
}
