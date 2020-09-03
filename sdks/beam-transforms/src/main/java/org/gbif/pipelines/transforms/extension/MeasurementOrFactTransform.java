package org.gbif.pipelines.transforms.extension;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT;

import java.time.Instant;
import java.util.Optional;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.MeasurementOrFactInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Measurements_or_facts extension, reads an avro, writes an
 * avro, maps from value to keyValue and transforms form{@link ExtendedRecord} to {@link
 * MeasurementOrFactRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link MeasurementOrFactRecord} using {@link
 * ExtendedRecord} as a source and {@link MeasurementOrFactInterpreter} as interpretation steps
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
public class MeasurementOrFactTransform extends Transform<ExtendedRecord, MeasurementOrFactRecord> {

  public MeasurementOrFactTransform() {
    super(
        MeasurementOrFactRecord.class,
        MEASUREMENT_OR_FACT,
        MeasurementOrFactTransform.class.getName(),
        MEASUREMENT_OR_FACT_RECORDS_COUNT);
  }

  public static MeasurementOrFactTransform create() {
    return new MeasurementOrFactTransform();
  }

  /**
   * Maps {@link MeasurementOrFactRecord} to key value, where key is {@link
   * MeasurementOrFactRecord#getId}
   */
  public MapElements<MeasurementOrFactRecord, KV<String, MeasurementOrFactRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, MeasurementOrFactRecord>>() {})
        .via((MeasurementOrFactRecord mr) -> KV.of(mr.getId(), mr));
  }

  public MeasurementOrFactTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<MeasurementOrFactRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                MeasurementOrFactRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(
            er ->
                Optional.ofNullable(
                        er.getExtensions().get(Extension.MEASUREMENT_OR_FACT.getRowType()))
                    .filter(l -> !l.isEmpty())
                    .isPresent())
        .via(MeasurementOrFactInterpreter::interpret)
        .getOfNullable();
  }
}
