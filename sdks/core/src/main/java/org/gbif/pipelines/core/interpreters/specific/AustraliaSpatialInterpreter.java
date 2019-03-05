package org.gbif.pipelines.core.interpreters.specific;

import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** TODO:DOC */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AustraliaSpatialInterpreter {

  /** TODO:DOC */
  public static void interpret(LocationRecord er, AustraliaSpatialRecord asr) {
    // TODO: REAL IMPL HERE
    asr.setField(er.getCountry());
  }
}
