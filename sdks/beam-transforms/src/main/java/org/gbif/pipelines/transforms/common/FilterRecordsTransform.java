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
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

/** Filter uses invalid BasicRecord collection as a source to find and skip ExtendedRecord record */
@AllArgsConstructor(staticName = "create")
public class FilterRecordsTransform implements Serializable {

  private static final long serialVersionUID = 2953351237274578375L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<IdentifierRecord> idTag;

  /** Filters the records by discarding the results that have an invalid {@link IdentifierRecord} */
  public SingleOutput<KV<String, CoGbkResult>, ExtendedRecord> filter() {

    DoFn<KV<String, CoGbkResult>, ExtendedRecord> fn =
        new DoFn<KV<String, CoGbkResult>, ExtendedRecord>() {

          private final Counter counter =
              Metrics.counter(FilterRecordsTransform.class, FILTER_ER_BASED_ON_GBIF_ID);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();

            ExtendedRecord er = v.getOnly(erTag, null);
            IdentifierRecord id = v.getOnly(idTag, null);
            if (er != null && id != null && id.getInternalId() != null) {
              c.output(er);
              counter.inc();
            }
          }
        };

    return ParDo.of(fn);
  }
}
