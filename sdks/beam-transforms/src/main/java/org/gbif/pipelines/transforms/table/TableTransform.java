package org.gbif.pipelines.transforms.table;

import java.io.Serializable;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

@SuppressWarnings("ConstantConditions")
@Builder
public class TableTransform<T extends SpecificRecordBase> implements Serializable {

  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<BasicRecord> basicRecordTag;

  @NonNull private final String counterName;

  @NonNull private final Class<T> clazz;

  @NonNull
  private final SerializableBiFunction<BasicRecord, ExtendedRecord, Optional<T>> converterFn;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, T> converter() {
    DoFn<KV<String, CoGbkResult>, T> fn =
        new DoFn<KV<String, CoGbkResult>, T>() {

          private final Counter counter = Metrics.counter(TableTransform.class, counterName);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());

            BasicRecord br = v.getOnly(basicRecordTag, BasicRecord.newBuilder().setId(k).build());

            converterFn
                .apply(br, er)
                .ifPresent(
                    record -> {
                      c.output(record);
                      counter.inc();
                    });
          }
        };
    return ParDo.of(fn);
  }

  public AvroIO.Write<T> write(String toPath, Integer numShards) {
    AvroIO.Write<T> write =
        AvroIO.write(clazz)
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
