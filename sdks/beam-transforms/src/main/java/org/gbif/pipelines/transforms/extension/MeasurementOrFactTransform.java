package org.gbif.pipelines.transforms.extension;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.converters.MeasurementOrFactConverter;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.MeasurementOrFactInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Measurements_or_facts extension, reads an avro, writes an
 * avro, maps from value to keyValue and transforms from {@link ExtendedRecord} to {@link
 * MeasurementOrFactRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link MeasurementOrFactRecord} using {@link
 * ExtendedRecord} as a source and {@link MeasurementOrFactInterpreter} as interpretation steps
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
public class MeasurementOrFactTransform extends Transform<ExtendedRecord, MeasurementOrFactRecord> {

  @Builder.Default private boolean useDynamicPropertiesInterpretation = false;

  @Builder(buildMethodName = "create")
  private MeasurementOrFactTransform(boolean useDynamicPropertiesInterpretation) {
    super(
        MeasurementOrFactRecord.class,
        MEASUREMENT_OR_FACT,
        MeasurementOrFactTransform.class.getName(),
        MEASUREMENT_OR_FACT_RECORDS_COUNT);

    this.useDynamicPropertiesInterpretation = useDynamicPropertiesInterpretation;
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

    ExtendedRecord record = source;

    List<Map<String, String>> dynExts = Collections.emptyList();
    if (useDynamicPropertiesInterpretation) {
      dynExts = MeasurementOrFactConverter.extractFromDynamicProperties(source);
    }

    if (!dynExts.isEmpty()) {
      record = ExtendedRecord.newBuilder(source).build();
      if (hasExtension(record, Extension.MEASUREMENT_OR_FACT)) {
        record.getExtensions().get(Extension.MEASUREMENT_OR_FACT.getRowType()).addAll(dynExts);
      } else {
        record.getExtensions().put(Extension.MEASUREMENT_OR_FACT.getRowType(), dynExts);
      }
    }

    if (!hasExtension(record, Extension.MEASUREMENT_OR_FACT)
        && !hasExtension(record, Extension.EXTENDED_MEASUREMENT_OR_FACT)) {
      return Optional.empty();
    }

    return Interpretation.from(record)
        .to(
            er ->
                MeasurementOrFactRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .via(MeasurementOrFactInterpreter::interpret)
        .getOfNullable();
  }
}
