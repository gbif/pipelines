package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GBIF_ID_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_INVALID;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EVENT_IDENTIFIER;

import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link GbifIdRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
public class GbifEventIdTransform extends Transform<ExtendedRecord, GbifIdRecord> {

  @Builder(buildMethodName = "create")
  private GbifEventIdTransform() {
    super(
        GbifIdRecord.class,
        EVENT_IDENTIFIER,
        GbifEventIdTransform.class.getName(),
        GBIF_ID_RECORDS_COUNT);
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
                  Optional.ofNullable(gr.getGbifId()).map(Object::toString).orElse(GBIF_ID_INVALID);
              return KV.of(key, gr);
            });
  }

  public GbifEventIdTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup can be applied only to void method */
  public GbifEventIdTransform init() {
    return this;
  }

  @Override
  public Optional<GbifIdRecord> convert(ExtendedRecord source) {

    GbifIdRecord gr =
        GbifIdRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    return Interpretation.from(source)
        .to(gr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(GbifIdInterpreter.interpretCopyGbifId())
        .getOfNullable();
  }
}
