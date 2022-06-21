package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EVENT_CORE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EVENT;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.CoreInterpreter;
import org.gbif.pipelines.core.interpreters.core.VocabularyInterpreter;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DWC Event, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form {@link ExtendedRecord} to {@link EventCoreRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#event"/>
 */
public class EventCoreTransform extends Transform<ExtendedRecord, EventCoreRecord> {

  private final SerializableSupplier<VocabularyService> vocabularyServiceSupplier;
  private VocabularyService vocabularyService;

  @Setter private PCollectionView<Map<String, Map<String, String>>> erWithParentsView;

  @Builder(buildMethodName = "create")
  private EventCoreTransform(
      SerializableSupplier<VocabularyService> vocabularyServiceSupplier,
      PCollectionView<Map<String, Map<String, String>>> erWithParentsView) {
    super(
        EventCoreRecord.class, EVENT, EventCoreTransform.class.getName(), EVENT_CORE_RECORDS_COUNT);
    this.vocabularyServiceSupplier = vocabularyServiceSupplier;
    this.erWithParentsView = erWithParentsView;
  }

  /** Maps {@link EventCoreRecord} to key value, where key is {@link EventCoreRecord#getId} */
  public MapElements<EventCoreRecord, KV<String, EventCoreRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, EventCoreRecord>>() {})
        .via((EventCoreRecord br) -> KV.of(br.getId(), br));
  }

  public EventCoreTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (vocabularyService == null && vocabularyServiceSupplier != null) {
      vocabularyService = vocabularyServiceSupplier.get();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public EventCoreTransform init() {
    setup();
    return this;
  }

  /** Beam @Teardown closes initialized resources */
  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (vocabularyService != null) {
      vocabularyService.close();
    }
  }

  @Override
  public Optional<EventCoreRecord> convert(ExtendedRecord source) {
    throw new IllegalArgumentException("Method is not implemented!");
  }

  @Override
  public SingleOutput<ExtendedRecord, EventCoreRecord> interpret() {
    return ParDo.of(this).withSideInputs(erWithParentsView);
  }

  @Override
  @ProcessElement
  public void processElement(ProcessContext c) {
    processElement(c.element(), c.sideInput(erWithParentsView)).ifPresent(c::output);
  }

  public Optional<EventCoreRecord> processElement(
      ExtendedRecord source, Map<String, Map<String, String>> erWithParents) {
    return Interpretation.from(source)
        .to(
            EventCoreRecord.newBuilder()
                .setId(source.getId())
                .setCreated(Instant.now().toEpochMilli())
                .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(VocabularyInterpreter.interpretEventType(vocabularyService))
        .via((e, r) -> CoreInterpreter.interpretReferences(e, r, r::setReferences))
        .via((e, r) -> CoreInterpreter.interpretSampleSizeUnit(e, r::setSampleSizeUnit))
        .via((e, r) -> CoreInterpreter.interpretSampleSizeValue(e, r::setSampleSizeValue))
        .via((e, r) -> CoreInterpreter.interpretLicense(e, r::setLicense))
        .via((e, r) -> CoreInterpreter.interpretDatasetID(e, r::setDatasetID))
        .via((e, r) -> CoreInterpreter.interpretDatasetName(e, r::setDatasetName))
        .via((e, r) -> CoreInterpreter.interpretSamplingProtocol(e, r::setSamplingProtocol))
        .via((e, r) -> CoreInterpreter.interpretParentEventID(e, r::setParentEventID))
        .via((e, r) -> CoreInterpreter.interpretLocationID(e, r::setLocationID))
        .via(CoreInterpreter.interpretLineages(erWithParents, vocabularyService))
        .getOfNullable();
  }
}
