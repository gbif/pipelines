package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GBIF_ID_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_INVALID;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
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
public class GbifIdTransform extends Transform<ExtendedRecord, IdentifierRecord> {

  @Getter private final String absentName;
  @Getter private final Set<String> allNames = new HashSet<>(3);
  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;
  private final boolean generateIdIfAbsent;
  private final BiConsumer<ExtendedRecord, IdentifierRecord> gbifIdFn;
  private final SerializableSupplier<HBaseLockingKey> keygenServiceSupplier;

  private HBaseLockingKey keygenService;

  @Builder(buildMethodName = "create")
  private GbifIdTransform(
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      boolean generateIdIfAbsent,
      BiConsumer<ExtendedRecord, IdentifierRecord> gbifIdFn,
      SerializableSupplier<HBaseLockingKey> keygenServiceSupplier) {
    super(
        IdentifierRecord.class, IDENTIFIER, GbifIdTransform.class.getName(), GBIF_ID_RECORDS_COUNT);
    this.isTripletValid = isTripletValid;
    this.isOccurrenceIdValid = isOccurrenceIdValid;
    this.useExtendedRecordId = useExtendedRecordId;
    this.generateIdIfAbsent = generateIdIfAbsent;
    this.gbifIdFn = gbifIdFn;
    this.keygenServiceSupplier = keygenServiceSupplier;
    this.absentName = this.getBaseName() + "_absent";
    allNames.addAll(Arrays.asList(this.getBaseName(), this.getBaseInvalidName(), absentName));
  }

  /** Maps {@link IdentifierRecord} to key value, where key is {@link IdentifierRecord#getId} */
  public MapElements<IdentifierRecord, KV<String, IdentifierRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, IdentifierRecord>>() {})
        .via((IdentifierRecord ir) -> KV.of(ir.getId(), ir));
  }

  /**
   * Maps {@link IdentifierRecord} to key value, where key is {@link
   * IdentifierRecord#getInternalId()}
   */
  public MapElements<IdentifierRecord, KV<String, IdentifierRecord>> toGbifIdKv() {
    return MapElements.into(new TypeDescriptor<KV<String, IdentifierRecord>>() {})
        .via(
            (IdentifierRecord ir) -> {
              String key = Optional.ofNullable(ir.getInternalId()).orElse(GBIF_ID_INVALID);
              return KV.of(key, ir);
            });
  }

  public GbifIdTransform counterFn(SerializableConsumer<String> counterFn) {
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
  public GbifIdTransform init() {
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
  public Optional<IdentifierRecord> convert(ExtendedRecord source) {

    IdentifierRecord ir =
        IdentifierRecord.newBuilder()
            .setId(source.getId())
            .setFirstLoaded(Instant.now().toEpochMilli())
            .build();

    if (useExtendedRecordId && source.getCoreTerms().isEmpty()) {
      GbifIdInterpreter.interpretCopyGbifId().accept(source, ir);
    }

    return Interpretation.from(source)
        .to(ir)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(
            GbifIdInterpreter.interpretGbifId(
                keygenService,
                isTripletValid,
                isOccurrenceIdValid,
                useExtendedRecordId,
                generateIdIfAbsent,
                gbifIdFn))
        .getOfNullable();
  }
}
