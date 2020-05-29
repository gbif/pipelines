package org.gbif.pipelines.transforms.metadata;

import java.util.Optional;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.metadata.TaggedValuesInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAGGED_VALUES_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAGGED_VALUES;

/**
 * Beam level transformations for the GBIF tagged value in dataset metadata, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link TaggedValueRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link TaggedValueRecord} using {@link ExtendedRecord}
 * as a source and {@link TaggedValuesInterpreter} as interpretation steps
 */
@Slf4j
public class TaggedValuesTransform extends Transform<ExtendedRecord, TaggedValueRecord> {

  private PCollectionView<MetadataRecord> metadataView;

  private TaggedValuesTransform() {
    super(TaggedValueRecord.class, TAGGED_VALUES, TaggedValueRecord.class.getName(), TAGGED_VALUES_RECORDS_COUNT);
  }

  public static TaggedValuesTransform create() {
    return new TaggedValuesTransform();
  }

  @Override
  public Optional<TaggedValueRecord> convert(ExtendedRecord source) {
    throw new IllegalArgumentException("Method is not implemented!");
  }

  public TaggedValuesTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public ParDo.SingleOutput<ExtendedRecord, TaggedValueRecord> interpret(PCollectionView<MetadataRecord> metadataView) {
    this.metadataView = metadataView;
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
    Optional<TaggedValueRecord> result = Interpretation.from(source)
      .to(id -> TaggedValueRecord.newBuilder().setId(source.getId()).build())
      .via(TaggedValuesInterpreter.interpret(mdr))
      .get();

    result.ifPresent(r -> this.incCounter());

    return result;
  }

}
