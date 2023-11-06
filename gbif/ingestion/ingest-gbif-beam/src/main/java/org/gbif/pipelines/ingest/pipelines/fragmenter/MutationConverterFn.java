package org.gbif.pipelines.ingest.pipelines.fragmenter;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FRAGMENTER_COUNT;

import lombok.Builder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.RawRecord;

@Builder(buildMethodName = "create")
public class MutationConverterFn extends DoFn<RawRecord, Mutation> {

  private final Counter counter = Metrics.counter(MutationConverterFn.class, FRAGMENTER_COUNT);

  String datasetKey;
  Integer attempt;
  String protocol;

  @ProcessElement
  public void processElement(@Element RawRecord rr, OutputReceiver<Mutation> out) {
    Put fragmentPut = HbaseStore.createFragmentPut(datasetKey, attempt, protocol, rr);

    out.output(fragmentPut);

    counter.inc();
  }
}
