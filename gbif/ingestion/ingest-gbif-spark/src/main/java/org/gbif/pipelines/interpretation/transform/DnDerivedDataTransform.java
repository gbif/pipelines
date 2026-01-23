package org.gbif.pipelines.interpretation.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.interpreters.extension.DnaDerivedDataInterpreter;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DnDerivedDataTransform implements Serializable {

  private transient DnaDerivedDataInterpreter dnaDerivedDataInterpreter;

  public static DnDerivedDataTransform create() {
    return new DnDerivedDataTransform();
  }

  public DnaDerivedDataRecord convert(ExtendedRecord source) {

    if (dnaDerivedDataInterpreter == null) {
      dnaDerivedDataInterpreter = DnaDerivedDataInterpreter.builder().create();
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

    dnaDerivedDataInterpreter.interpret(source, dr);
    return dr;
  }
}
