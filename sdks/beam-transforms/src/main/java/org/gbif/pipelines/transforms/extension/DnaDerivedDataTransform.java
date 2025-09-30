package org.gbif.pipelines.transforms.extension;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.DNA_DERIVED_DATA;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DNA_DERIVED_DATA_RECORDS_COUNT;
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
import org.gbif.pipelines.core.interpreters.extension.DnaDerivedDataInterpreter;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DNA derived data extension, reads an avro, writes an avro,
 * maps from value to keyValue and transforms form {@link ExtendedRecord} to {@link
 * DnaDerivedDataRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link DnaDerivedDataRecord} using {@link
 * ExtendedRecord} as a source and {@link DnaDerivedDataInterpreter} as interpretation steps
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/dna_derived_data_2024-07-11.xml</a>
 */
public class DnaDerivedDataTransform extends Transform<ExtendedRecord, DnaDerivedDataRecord> {

  private DnaDerivedDataInterpreter dnaDerivedDataInterpreter;

  @Builder(buildMethodName = "create")
  private DnaDerivedDataTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        DnaDerivedDataRecord.class,
        DNA_DERIVED_DATA,
        DnaDerivedDataTransform.class.getName(),
        DNA_DERIVED_DATA_RECORDS_COUNT);
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (dnaDerivedDataInterpreter == null) {
      dnaDerivedDataInterpreter = DnaDerivedDataInterpreter.builder().create();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public DnaDerivedDataTransform init() {
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

  public DnaDerivedDataTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<DnaDerivedDataRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                DnaDerivedDataRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.DNA_DERIVED_DATA))
        .via(dnaDerivedDataInterpreter::interpret)
        .getOfNullable();
  }
}
