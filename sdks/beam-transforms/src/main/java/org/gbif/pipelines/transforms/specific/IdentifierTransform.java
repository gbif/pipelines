package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFIER_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER;

import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.Transform;

/** Beam level transformations for the DWC record fields mapping to internal identifier */
@Slf4j
public class IdentifierTransform extends Transform<ExtendedRecord, IdentifierRecord> {

  @Builder(buildMethodName = "create")
  private IdentifierTransform() {
    super(
        IdentifierRecord.class,
        IDENTIFIER,
        IdentifierTransform.class.getName(),
        IDENTIFIER_RECORDS_COUNT);
  }

  /** Maps {@link IdentifierRecord} to key value, where key is {@link IdentifierRecord#getId()} */
  public MapElements<IdentifierRecord, KV<String, IdentifierRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, IdentifierRecord>>() {})
        .via((IdentifierRecord ar) -> KV.of(ar.getId(), ar));
  }

  public IdentifierTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Setup
  public void setup() {
    // TODO: INIT OR DELETE
  }

  @Teardown
  public void tearDown() {
    // TODO: TEARDOWN OR DELETE
  }

  @Override
  public Optional<IdentifierRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            lr ->
                IdentifierRecord.newBuilder()
                    .setId(lr.getId())
                    .setFirstLoaded(Instant.now().toEpochMilli())
                    .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(
            (extendedRecord, identifierRecord) -> {
              // TODO: DELETE ME!
              identifierRecord.setInternalId(extendedRecord.getId());
            })
        // .via(IdentifierInterpreter::someInterpretation1)
        // .via(IdentifierInterpreter::someInterpretation2)
        // .via(IdentifierInterpreter::someInterpretation3)
        .getOfNullable();
  }
}
