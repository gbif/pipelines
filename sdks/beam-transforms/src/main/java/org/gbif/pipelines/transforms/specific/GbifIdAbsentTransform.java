package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GBIF_ID_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER;

import java.util.Optional;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link IdentifierRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
public class GbifIdAbsentTransform extends Transform<IdentifierRecord, IdentifierRecord> {

  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final SerializableSupplier<HBaseLockingKey> keygenServiceSupplier;

  private HBaseLockingKey keygenService;

  @Builder(buildMethodName = "create")
  private GbifIdAbsentTransform(
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      SerializableSupplier<HBaseLockingKey> keygenServiceSupplier) {
    super(
        IdentifierRecord.class,
        IDENTIFIER,
        GbifIdAbsentTransform.class.getName(),
        GBIF_ID_RECORDS_COUNT);
    this.isTripletValid = isTripletValid;
    this.isOccurrenceIdValid = isOccurrenceIdValid;
    this.keygenServiceSupplier = keygenServiceSupplier;
  }

  public PCollection<IdentifierRecord> emptyCollection(Pipeline p) {
    return Create.empty(TypeDescriptor.of(IdentifierRecord.class)).expand(PBegin.in(p));
  }

  public GbifIdAbsentTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (keygenService == null && keygenServiceSupplier != null) {
      keygenService = keygenServiceSupplier.get();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public GbifIdAbsentTransform init() {
    setup();
    return this;
  }

  /** Beam @Teardown closes initialized resources */
  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (keygenService != null) {
      keygenService.close();
    }
  }

  @Override
  public Optional<IdentifierRecord> convert(IdentifierRecord source) {
    return Interpretation.from(source)
        .to(IdentifierRecord.newBuilder(source).build())
        .when(ir -> ir.getInternalId() == null)
        .via(
            GbifIdInterpreter.interpretAbsentGbifId(
                keygenService, isTripletValid, isOccurrenceIdValid))
        .getOfNullable();
  }
}
