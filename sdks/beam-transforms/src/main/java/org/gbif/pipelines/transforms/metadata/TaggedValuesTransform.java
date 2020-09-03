package org.gbif.pipelines.transforms.metadata;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAGGED_VALUES_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAGGED_VALUES;

import java.util.Optional;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.metadata.TaggedValuesInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the GBIF tagged value in dataset metadata, reads an avro, writes
 * an avro, maps from value to keyValue and transforms form {@link ExtendedRecord} to {@link
 * TaggedValueRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TaggedValueRecord} using {@link
 * ExtendedRecord} as a source and {@link TaggedValuesInterpreter} as interpretation steps
 */
@Slf4j
public class TaggedValuesTransform extends Transform<ExtendedRecord, TaggedValueRecord> {

  @Setter private PCollectionView<MetadataRecord> metadataView;

  @Builder(buildMethodName = "create")
  private TaggedValuesTransform(PCollectionView<MetadataRecord> metadataView) {
    super(
        TaggedValueRecord.class,
        TAGGED_VALUES,
        TaggedValueRecord.class.getName(),
        TAGGED_VALUES_RECORDS_COUNT);
    this.metadataView = metadataView;
  }

  @Override
  public Optional<TaggedValueRecord> convert(ExtendedRecord source) {
    throw new IllegalArgumentException("Method is not implemented!");
  }

  public TaggedValuesTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public ParDo.SingleOutput<ExtendedRecord, TaggedValueRecord> interpret() {
    return ParDo.of(this).withSideInputs(metadataView);
  }

  /** Maps {@link TaggedValueRecord} to key value, where key is {@link TaggedValueRecord#getId} */
  public MapElements<TaggedValueRecord, KV<String, TaggedValueRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TaggedValueRecord>>() {})
        .via((TaggedValueRecord lr) -> KV.of(lr.getId(), lr));
  }

  @Override
  @ProcessElement
  public void processElement(ProcessContext c) {
    processElement(c.element(), c.sideInput(metadataView)).ifPresent(c::output);
  }

  public Optional<TaggedValueRecord> processElement(ExtendedRecord source, MetadataRecord mdr) {
    return Interpretation.from(source)
        .to(id -> TaggedValueRecord.newBuilder().setId(source.getId()).build())
        .via(TaggedValuesInterpreter.interpret(mdr))
        .via(r -> this.incCounter())
        .getOfNullable();
  }
}
