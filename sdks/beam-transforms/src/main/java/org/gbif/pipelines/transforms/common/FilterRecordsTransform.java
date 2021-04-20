package org.gbif.pipelines.transforms.common;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FILTER_ER_BASED_ON_GBIF_ID;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.pojo.ErBrContainer;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Filter uses invalid BasicRecord collection as a source to find and skip ExtendedRecord record */
@AllArgsConstructor(staticName = "create")
public class FilterRecordsTransform implements Serializable {

  private static final long serialVersionUID = 2953351237274578363L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<BasicRecord> brTag;

  /** Filters the records by discarding the results that have an invalid {@link BasicRecord} */
  public SingleOutput<KV<String, CoGbkResult>, ErBrContainer> filter() {

    DoFn<KV<String, CoGbkResult>, ErBrContainer> fn =
        new DoFn<KV<String, CoGbkResult>, ErBrContainer>() {

          private final Counter counter =
              Metrics.counter(FilterRecordsTransform.class, FILTER_ER_BASED_ON_GBIF_ID);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();

            ExtendedRecord er = v.getOnly(erTag, null);
            BasicRecord br = v.getOnly(brTag, null);
            if (er != null && br != null && br.getCreated() != null) {
              c.output(ErBrContainer.create(er, br));
              counter.inc();
            }
          }
        };

    return ParDo.of(fn);
  }

  /** It extracts the {@link ExtendedRecord} from a {@link CoGbkResult}. */
  public SingleOutput<ErBrContainer, ExtendedRecord> extractExtendedRecords() {
    DoFn<ErBrContainer, ExtendedRecord> fn =
        new DoFn<ErBrContainer, ExtendedRecord>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            ErBrContainer v = c.element();
            c.output(v.getEr());
          }
        };

    return ParDo.of(fn);
  }
}
