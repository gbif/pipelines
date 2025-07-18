package org.gbif.pipelines.transforms.extension;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.HUMBOLDT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.HUMBOLDT_RECORDS_COUNT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.HumboldtInterpreter;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Humboldt extension, reads an avro, writes an avro, maps from
 * value to keyValue and transforms form {@link ExtendedRecord} to {@link HumboldtRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link HumboldtRecord} using {@link ExtendedRecord}
 * as a source and TODO {@link HumboldtInterpreter} as interpretation steps
 *
 * @see <a href="https://rs.gbif.org/extension/eco/Humboldt_2024-04-16.xml>Humboldt extension
 *     definition</a>
 */
public class HumboldtTransform extends Transform<ExtendedRecord, HumboldtRecord> {

  private HumboldtInterpreter humboldtInterpreter;

  @Builder(buildMethodName = "create")
  private HumboldtTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        HumboldtRecord.class, HUMBOLDT, HumboldtTransform.class.getName(), HUMBOLDT_RECORDS_COUNT);
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (humboldtInterpreter == null) {
      humboldtInterpreter = HumboldtInterpreter.builder().create();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public HumboldtTransform init() {
    setup();
    return this;
  }

  /**
   * Maps {@link DnaDerivedDataRecord} to key value, where key is {@link DnaDerivedDataRecord#getId}
   */
  public MapElements<DnaDerivedDataRecord, KV<String, DnaDerivedDataRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, DnaDerivedDataRecord>>() {})
        .via((DnaDerivedDataRecord dr) -> KV.of(dr.getId(), dr));
  }

  public HumboldtTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<HumboldtRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                HumboldtRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.HUMBOLDT))
        .via(humboldtInterpreter::interpret)
        .getOfNullable();
  }
}
