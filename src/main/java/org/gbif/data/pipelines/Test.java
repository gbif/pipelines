package org.gbif.data.pipelines;

import org.gbif.data.io.avro.ExtendedRecord;
import org.gbif.data.io.avro.UntypedOccurrence;
import org.gbif.data.pipelines.io.dwca.DwCAIO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;

/**
 * First demonstration only!
 */
public class Test {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    //options.setRunner(DirectRunner.class); // first testing only
    options.setRunner(SparkRunner.class);

    Pipeline p = Pipeline.create(options);

    p.getCoderRegistry().registerCoderForClass(UntypedOccurrence.class, AvroCoder.of(UntypedOccurrence.class));

    PCollection<ExtendedRecord> rawRecords = p.apply(
      "ReadRawRecords", DwCAIO.Read.withPaths(Paths.get("hdfs:///tmp/dwca.zip"), Paths.get("hdfs:///tmp/working")));

    // Repartition data for a test
    //rawRecords.apply(Partition.of(10, new RecordPartitioner()));

    // Convert the record type into an UntypedOccurrence record
    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "ParseRawToDwC", ParDo.of(new ParseDwC()));

    // Write the file to Avro
    verbatimRecords.apply(AvroIO.write(UntypedOccurrence.class).to("output/transformed"));

    // Get a term frequency
    /*
    This is half done.  But 1) blows memory (it shouldn't) and 2) the Coder is not written.
    PCollection<Map<String, Integer>> termFrequency = rawRecords
      .apply(Combine.globally(new TFAccumulator()));
    */

    PipelineResult result = p.run();
    // Note: can read result state here (e.g. a web app polling for metrics would do this)
    result.waitUntilFinish();
  }

  /**
   * Hashcode partitioner (for demo only)
   */
  public static class RecordPartitioner implements Partition.PartitionFn<ExtendedRecord> {
    @Override
    public int partitionFor(ExtendedRecord elem, int numPartitions) {
      int hc = elem.hashCode();
      if (hc<0) hc*=-1;
      return hc % numPartitions;
    }
  }

  /**
   * Term frequency accumulator
   */
  public static class TFAccumulator extends Combine.CombineFn<ExtendedRecord, TFAccumulator.Accum, Map<String, Integer>> {
    public static class Accum {
      private final Map<String,Integer> frequency = new HashMap<>();
    }

    @Override
    public Coder<Accum> getAccumulatorCoder(
      CoderRegistry registry, Coder<ExtendedRecord> inputCoder
    ) throws CannotProvideCoderException {
      return new Coder<Accum>() {
        @Override
        public void encode(Accum value, OutputStream outStream) throws CoderException, IOException {
          // TODO Perhaps we use Avro exclusively?
        }

        @Override
        public Accum decode(InputStream inStream) throws CoderException, IOException {
          // TODO Perhaps we use Avro exclusively?
          return new Accum();
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
          return Collections.EMPTY_LIST;
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
        }
      };
    }

    @Override
    public Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public Accum addInput(Accum accumulator, ExtendedRecord input) {
      for (CharSequence term : input.getCoreTerms().keySet()) {
        accumulator.frequency.merge(term.toString(), 1, (oldValue, one) -> oldValue + one);
      }
      return accumulator;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accumulators) {
      Accum result = null;
      for (Accum accum : accumulators) {
        if (result == null) {
          result = accum;
        } else {
          for (Map.Entry<String,Integer> e : accum.frequency.entrySet()) {
            result.frequency.merge(e.getKey(), e.getValue(), (oldValue, increment) -> oldValue + increment);
          }
        }
      }
      return result == null ? new Accum() : result; // defensive coding
    }

    @Override
    public Map<String, Integer> extractOutput(Accum accumulator) {
      return accumulator.frequency;
    }
  }
}
