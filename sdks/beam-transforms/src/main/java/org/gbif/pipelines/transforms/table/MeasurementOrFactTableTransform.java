package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;

import java.io.Serializable;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactTable;
import org.gbif.pipelines.transforms.Transform;

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
@SuppressWarnings("ConstantConditions")
@Builder
public class MeasurementOrFactTableTransform implements Serializable {

  private static final long serialVersionUID = 4705389346756029671L;

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
                morf -> {
                  c.output(morf);
                  counter.inc();
                });
          }
        };
    return ParDo.of(fn);
  }

  /**
   * Writes {@link MeasurementOrFactTable} *.avro files to path, data will be split into several
   * files, uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public AvroIO.Write<MeasurementOrFactTable> write(String toPath, Integer numShards) {
    AvroIO.Write<MeasurementOrFactTable> write =
        AvroIO.write(MeasurementOrFactTable.class)
            .to(toPath)
            .withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION)
            .withCodec(Transform.getBaseCodec());

    if (numShards == null && numShards <= 0) {
      return write;
    } else {
      int shards = -Math.floorDiv(-numShards, 2);
      return write.withNumShards(shards);
    }
  }
}
