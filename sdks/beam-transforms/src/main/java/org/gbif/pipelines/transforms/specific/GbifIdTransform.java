package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GBIF_ID_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GBIF_ID;

import java.time.Instant;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link GbifIdRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
public class GbifIdTransform extends Transform<ExtendedRecord, GbifIdRecord> {

  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;
  private final boolean generateIdIfAbsent;
  private final BiConsumer<ExtendedRecord, GbifIdRecord> gbifIdFn;
  private final SerializableSupplier<HBaseLockingKeyService> keygenServiceSupplier;

  private HBaseLockingKeyService keygenService;

  @Builder(buildMethodName = "create")
  private GbifIdTransform(
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      boolean generateIdIfAbsent,
      BiConsumer<ExtendedRecord, GbifIdRecord> gbifIdFn,
      SerializableSupplier<HBaseLockingKeyService> keygenServiceSupplier) {
    super(GbifIdRecord.class, GBIF_ID, GbifIdTransform.class.getName(), GBIF_ID_RECORDS_COUNT);
    this.isTripletValid = isTripletValid;
    this.isOccurrenceIdValid = isOccurrenceIdValid;
    this.useExtendedRecordId = useExtendedRecordId;
    this.generateIdIfAbsent = generateIdIfAbsent;
    this.gbifIdFn = gbifIdFn;
    this.keygenServiceSupplier = keygenServiceSupplier;
  }

  /** Maps {@link GbifIdRecord} to key value, where key is {@link GbifIdRecord#getId} */
  public MapElements<GbifIdRecord, KV<String, GbifIdRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, GbifIdRecord>>() {})
        .via((GbifIdRecord gr) -> KV.of(gr.getId(), gr));
  }

  /** Maps {@link GbifIdRecord} to key value, where key is {@link GbifIdRecord#getGbifId()} */
  public MapElements<GbifIdRecord, KV<String, GbifIdRecord>> toGbifIdKv() {
    return MapElements.into(new TypeDescriptor<KV<String, GbifIdRecord>>() {})
        .via(
            (GbifIdRecord gr) -> {
              String key =
                  Optional.ofNullable(gr.getGbifId())
                      .map(Object::toString)
                      .orElse(GbifIdInterpreter.GBIF_ID_INVALID);
              return KV.of(key, gr);
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
  public Optional<GbifIdRecord> convert(ExtendedRecord source) {

    GbifIdRecord gr =
        GbifIdRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (useExtendedRecordId && source.getCoreTerms().isEmpty()) {
      GbifIdInterpreter.interpretCopyGbifId().accept(source, gr);
    }

    return Interpretation.from(source)
        .to(gr)
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
