package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.pojo.ErIdrMdrContainer;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.CheckTransforms;

@SuppressWarnings("ConstantConditions")
public abstract class TableTransform<T extends SpecificRecordBase>
    extends DoFn<KV<String, CoGbkResult>, T> {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  @NonNull private final InterpretationType recordType;

  @NonNull private final Class<T> clazz;

  @NonNull private final SerializableFunction<ErIdrMdrContainer, List<T>> convertFn;

  @NonNull private TupleTag<ExtendedRecord> extendedRecordTag;

  @NonNull private TupleTag<IdentifierRecord> identifierRecordTag;

  @NonNull private PCollectionView<MetadataRecord> metadataView;

  @NonNull private String path;

  @NonNull private Integer numShards;

  @NonNull private Set<String> types;

  private final Counter counter;

  public TableTransform(
      Class<T> clazz,
      InterpretationType recordType,
      String counterNamespace,
      String counterName,
      SerializableFunction<ErIdrMdrContainer, List<T>> convertFn) {
    this.clazz = clazz;
    this.recordType = recordType;
    this.counter = Metrics.counter(counterNamespace, counterName);
    this.convertFn = convertFn;
  }

  public TableTransform<T> setExtendedRecordTag(TupleTag<ExtendedRecord> extendedRecordTag) {
    this.extendedRecordTag = extendedRecordTag;
    return this;
  }

  public TableTransform<T> setIdentifierRecordTag(TupleTag<IdentifierRecord> identifierRecordTag) {
    this.identifierRecordTag = identifierRecordTag;
    return this;
  }

  public TableTransform<T> setMetadataRecord(PCollectionView<MetadataRecord> metadataView) {
    this.metadataView = metadataView;
    return this;
  }

  public TableTransform<T> setPath(String path) {
    this.path = path;
    return this;
  }

  public TableTransform<T> setNumShards(Integer numShards) {
    this.numShards = numShards;
    return this;
  }

  public TableTransform<T> setTypes(Set<String> types) {
    this.types = types;
    return this;
  }

  public Optional<PCollection<KV<String, CoGbkResult>>> check(
      PCollection<KV<String, CoGbkResult>> pCollection) {
    return CheckTransforms.checkRecordType(types, recordType)
        ? Optional.of(pCollection)
        : Optional.empty();
  }

  public void write(PCollection<KV<String, CoGbkResult>> pCollection) {
    if (CheckTransforms.checkRecordType(types, recordType)) {
      pCollection
          .apply("Convert to " + recordType.name(), this.convert())
          .apply("Write " + recordType.name(), this.write());
    }
  }

  public AvroIO.Write<T> write() {
    AvroIO.Write<T> write =
        AvroIO.write(clazz).to(path).withSuffix(AVRO_EXTENSION).withCodec(BASE_CODEC);

    if (numShards == null || numShards <= 0) {
      return write;
    } else {
      int shards = -Math.floorDiv(-numShards, 2);
      return write.withNumShards(shards);
    }
  }

  public SingleOutput<KV<String, CoGbkResult>, T> convert() {
    return ParDo.of(this).withSideInputs(metadataView);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    CoGbkResult v = c.element().getValue();
    String k = c.element().getKey();

    ExtendedRecord er = v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
    MetadataRecord mdr = c.sideInput(metadataView);
    IdentifierRecord id =
        v.getOnly(identifierRecordTag, IdentifierRecord.newBuilder().setId(k).build());

    convertFn
        .apply(ErIdrMdrContainer.create(er, id, mdr))
        .forEach(
            r -> {
              c.output(r);
              counter.inc();
            });
  }
}
