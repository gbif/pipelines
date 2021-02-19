package org.gbif.pipelines.transforms.extension;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;

import java.io.Serializable;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactTable;

/**
 * Beam level transformations for the Measurement_or_facts extension, reads an avro, writes an avro,
 * maps from value to keyValue and transforms from {@link ExtendedRecord} to {@link
 * MeasurementOrFactTable}.
 *
 * <p>ParDo runs sequence of interpretations for {@link MeasurementOrFactTable} using {@link
 * ExtendedRecord} as a source and {@link MeasurementOrFactTableConverter} as interpretation steps
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
@Builder
public class MeasurementOrFactTableTransform implements Serializable {

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<BasicRecord> basicRecordTag;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, MeasurementOrFactTable> converter() {
    DoFn<KV<String, CoGbkResult>, MeasurementOrFactTable> fn =
        new DoFn<KV<String, CoGbkResult>, MeasurementOrFactTable>() {

          private final Counter counter =
              Metrics.counter(
                  MeasurementOrFactTableTransform.class, MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());

            BasicRecord br = v.getOnly(basicRecordTag, BasicRecord.newBuilder().setId(k).build());

            Optional<MeasurementOrFactTable> record =
                MeasurementOrFactTableConverter.convert(br, er);
            record.ifPresent(
                mOrF -> {
                  c.output(mOrF);
                  counter.inc();
                });
          }
        };
    return ParDo.of(fn);
  }
}
