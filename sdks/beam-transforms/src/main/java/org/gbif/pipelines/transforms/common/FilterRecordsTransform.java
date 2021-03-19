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
@AllArgsConstructor(staticName = "create")
public class FilterRecordsTransform implements Serializable {

  private static final long serialVersionUID = 2953351237274578363L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<BasicRecord> brTag;

  /** Filters the records by discarding the results that have an invalid {@link BasicRecord} */
  public SingleOutput<KV<String, CoGbkResult>, CoGbkResult> filter() {

    DoFn<KV<String, CoGbkResult>, CoGbkResult> fn =
        new DoFn<KV<String, CoGbkResult>, CoGbkResult>() {

          private final Counter counter =
              Metrics.counter(FilterRecordsTransform.class, FILTER_ER_BASED_ON_GBIF_ID);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
            if (br != null && br.getCreated() == null) {
              c.output(v);
              counter.inc();
            }
          }
        };

    return ParDo.of(fn);
  }

  /**
   * It extracts the {@link ExtendedRecord} from a {@link CoGbkResult}.
   */
  public SingleOutput<CoGbkResult, ExtendedRecord> extractExtendedRecords() {
    DoFn<CoGbkResult, ExtendedRecord> fn =
        new DoFn<CoGbkResult, ExtendedRecord>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element();
            ExtendedRecord er = v.getOnly(erTag);
            c.output(er);
          }
        };

    return ParDo.of(fn);
  }
}
