package org.gbif.pipelines.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValueNullAware;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;

@Builder
@AllArgsConstructor
public class MultimediaTransform implements Serializable {

  private transient MultimediaInterpreter multimediaInterpreter;
  private final PipelinesConfig config;

  private MultimediaTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static MultimediaTransform create(PipelinesConfig config) {
    return new MultimediaTransform(config);
  }

  public MultimediaRecord convert(ExtendedRecord source) {
    if (source == null) {
      throw new IllegalArgumentException("Source ExtendedRecord cannot be null");
    }

    MultimediaRecord record =
        MultimediaRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    // Only proceed if the record has a multimedia extension or associated media
    if (!hasExtension(source, Extension.MULTIMEDIA)
        && !hasValueNullAware(source, DwcTerm.associatedMedia)) {
      return record;
    }

    if (multimediaInterpreter == null) {
      multimediaInterpreter =
          MultimediaInterpreter.builder().ordering(config.getDefaultDateFormat()).create();
    }

    multimediaInterpreter.interpret(source, record);
    MultimediaInterpreter.interpretAssociatedMedia(source, record);

    return record;
  }
}
