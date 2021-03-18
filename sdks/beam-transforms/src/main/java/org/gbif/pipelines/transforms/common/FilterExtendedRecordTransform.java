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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FILTER_ER_BASED_ON_GBIF_ID;

/** Filter uses invalid BasicRecord collection as a source to find and skip ExtendedRecord record */
@SuppressWarnings("ConstantConditions")
@AllArgsConstructor(staticName = "create")
public class FilterExtendedRecordTransform implements Serializable {

  private static final long serialVersionUID = 2953351237274578363L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<BasicRecord> brTag;

  public SingleOutput<KV<String, CoGbkResult>, KV<String, CoGbkResult>> filter() {

    DoFn<KV<String, CoGbkResult>, KV<String, CoGbkResult>> fn =
        new DoFn<KV<String, CoGbkResult>, KV<String, CoGbkResult>>() {

          private final Counter counter =
              Metrics.counter(FilterExtendedRecordTransform.class, FILTER_ER_BASED_ON_GBIF_ID);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
            if (br.getCreated() == null) {
              c.output(c.element());
              counter.inc();
            }
          }
        };

    return ParDo.of(fn);
  }
}
