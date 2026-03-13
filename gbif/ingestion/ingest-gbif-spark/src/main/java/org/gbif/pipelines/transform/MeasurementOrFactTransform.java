package org.gbif.pipelines.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.extension.MeasurementOrFactInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;

public class MeasurementOrFactTransform implements Serializable {

  private final PipelinesConfig config;

  private MeasurementOrFactTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static MeasurementOrFactTransform create(PipelinesConfig config) {
    return new MeasurementOrFactTransform(config);
  }

  public MeasurementOrFactRecord convert(ExtendedRecord source) {

    MeasurementOrFactRecord r =
        MeasurementOrFactRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (!hasExtension(source, Extension.MEASUREMENT_OR_FACT)
        && !hasExtension(source, Extension.EXTENDED_MEASUREMENT_OR_FACT)) {
      return r;
    }

    MeasurementOrFactInterpreter.interpret(source, r);
    return r;
  }
}
