package org.gbif.pipelines.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.extension.DnaDerivedDataInterpreter;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.factory.VocabularyServiceFactory;

public class DnaDerivedDataTransform implements Serializable {

  private final PipelinesConfig config;

  private DnaDerivedDataTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static DnaDerivedDataTransform create(PipelinesConfig config) {
    return new DnaDerivedDataTransform(config);
  }

  public DnaDerivedDataRecord convert(ExtendedRecord source) {
    if (source == null) {
      throw new IllegalArgumentException("ExtendedRecord is null");
    }

    DnaDerivedDataRecord dr =
        DnaDerivedDataRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (source.getExtensions() == null || source.getExtensions().isEmpty()) {
      return dr;
    }

    if (!hasExtension(source, Extension.DNA_DERIVED_DATA)) {
      return dr;
    }

    var vocabularyService = VocabularyServiceFactory.getInstance(config);

    DnaDerivedDataInterpreter dnaDerivedDataInterpreter =
        DnaDerivedDataInterpreter.builder().vocabularyService(vocabularyService).create();

    dnaDerivedDataInterpreter.interpret(source, dr);

    return dr;
  }
}
