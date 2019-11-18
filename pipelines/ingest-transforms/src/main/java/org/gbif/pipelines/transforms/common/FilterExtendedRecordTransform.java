package org.gbif.pipelines.transforms.common;

import java.io.Serializable;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FILTER_ER_BASED_ON_GBIF_ID;

@AllArgsConstructor(staticName = "create")
public class FilterExtendedRecordTransform implements Serializable {

  private static final long serialVersionUID = 2953351237274578362L;

  // Core
  @NonNull
  private final TupleTag<ExtendedRecord> erTag;
  @NonNull
  private final TupleTag<BasicRecord> brTag;

  public SingleOutput<KV<String, CoGbkResult>, ExtendedRecord> filter() {

    DoFn<KV<String, CoGbkResult>, ExtendedRecord> fn = new DoFn<KV<String, CoGbkResult>, ExtendedRecord>() {

      private final Counter counter = Metrics.counter(FilterExtendedRecordTransform.class, FILTER_ER_BASED_ON_GBIF_ID);

      @ProcessElement
      public void processElement(ProcessContext c) {
        CoGbkResult v = c.element().getValue();
        String k = c.element().getKey();

        // Core
        ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
        if (v.getOnly(brTag) == null) {
          c.output(er);
        }

        counter.inc();
      }
    };

    return ParDo.of(fn);
  }
}
