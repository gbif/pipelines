package org.gbif.pipelines.transform;

import java.io.Serializable;
import java.time.Instant;
import org.gbif.pipelines.core.interpreters.core.IdentifierInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

public class IdentifierTransform implements Serializable {

  public static IdentifierTransform create() {
    return new IdentifierTransform();
  }

  public IdentifierRecord convert(ExtendedRecord source, String datasetKey) {

    IdentifierRecord ir =
        IdentifierRecord.newBuilder()
            .setId(source.getId())
            .setFirstLoaded(Instant.now().toEpochMilli())
            .build();
    IdentifierInterpreter.interpretInternalId(datasetKey).accept(source, ir);
    return ir;
  }
}
