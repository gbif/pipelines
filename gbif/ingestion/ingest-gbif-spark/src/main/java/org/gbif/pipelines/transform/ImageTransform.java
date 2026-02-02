package org.gbif.pipelines.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.extension.ImageInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;

@Builder
@AllArgsConstructor
public class ImageTransform implements Serializable {

  private transient ImageInterpreter imageInterpreter;
  private final PipelinesConfig config;

  private ImageTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static ImageTransform create(PipelinesConfig config) {
    return new ImageTransform(config);
  }

  public ImageRecord convert(ExtendedRecord source) {

    if (source == null) {
      throw new IllegalArgumentException("Source ExtendedRecord cannot be null");
    }

    ImageRecord record =
        ImageRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (!hasExtension(source, Extension.IMAGE)) {
      return record;
    }

    if (imageInterpreter == null) {
      imageInterpreter =
          ImageInterpreter.builder().orderings(config.getDefaultDateFormat()).create();
    }

    imageInterpreter.interpret(source, record);

    return record;
  }
}
