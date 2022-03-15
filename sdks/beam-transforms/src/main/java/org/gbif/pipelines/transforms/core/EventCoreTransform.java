package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EVENT_CORE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EVENT_CORE;

import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.EventCoreInterpreter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DWC Event, reads an avro, writs an avro, maps from value to
 * keyValue and transforms form {@link ExtendedRecord} to {@link EventCoreRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#event"/>
 */
public class EventCoreTransform extends Transform<ExtendedRecord, EventCoreRecord> {

  @Builder(buildMethodName = "create")
  private EventCoreTransform() {
    super(
        EventCoreRecord.class,
        EVENT_CORE,
        EventCoreTransform.class.getName(),
        EVENT_CORE_RECORDS_COUNT);
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
    // TODO: INIT RESOURCES OR DELETE THE METHOD
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
    // TODO: CLOSE RESOURCES OR DELETE THE METHOD
  }

  @Override
  public Optional<EventCoreRecord> convert(ExtendedRecord source) {

    return Interpretation.from(source)
        .to(
            EventCoreRecord.newBuilder()
                .setId(source.getId())
                .setCreated(Instant.now().toEpochMilli())
                .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(EventCoreInterpreter::interpretReferences)
        .via(EventCoreInterpreter::interpretSamplingProtocol)
        .via(EventCoreInterpreter::interpretSampleSizeUnit)
        .via(EventCoreInterpreter::interpretSampleSizeValue)
        .via(EventCoreInterpreter::interpretLicense)
        .via(EventCoreInterpreter::interpretDatasetID)
        .via(EventCoreInterpreter::interpretDatasetName)
        .getOfNullable();
  }
}
