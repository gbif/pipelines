package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFICATION_TABLE_RECORDS_COUNT;

import java.io.Serializable;
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
import org.gbif.pipelines.core.converters.IdentificationTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.IdentificationTable;
import org.gbif.pipelines.transforms.Transform;

@SuppressWarnings("ConstantConditions")
@Builder
public class IdentificationTableTransform implements Serializable {

  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<BasicRecord> basicRecordTag;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, IdentificationTable> converter() {
    DoFn<KV<String, CoGbkResult>, IdentificationTable> fn =
        new DoFn<KV<String, CoGbkResult>, IdentificationTable>() {

          private final Counter counter =
              Metrics.counter(
                  IdentificationTableTransform.class, IDENTIFICATION_TABLE_RECORDS_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());

            BasicRecord br = v.getOnly(basicRecordTag, BasicRecord.newBuilder().setId(k).build());

            IdentificationTableConverter.convert(br, er)
                .ifPresent(
                    record -> {
                      c.output(record);
                      counter.inc();
                    });
          }
        };
    return ParDo.of(fn);
  }

  public AvroIO.Write<IdentificationTable> write(String toPath, Integer numShards) {
    AvroIO.Write<IdentificationTable> write =
        AvroIO.write(IdentificationTable.class)
            .to(toPath)
            .withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION)
            .withCodec(Transform.getBaseCodec());

    if (numShards == null || numShards <= 0) {
      return write;
    } else {
      int shards = -Math.floorDiv(-numShards, 2);
      return write.withNumShards(shards);
    }
  }
}
