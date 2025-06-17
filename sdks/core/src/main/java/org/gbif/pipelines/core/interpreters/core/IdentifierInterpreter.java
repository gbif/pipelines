package org.gbif.pipelines.core.interpreters.core;

import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.core.interpreters.model.IdentifierRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IdentifierInterpreter {

  /** Internal id interpretation. */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretInternalId(
      String datasetKey) {
    return (er, ir) -> {
      String id = er.getId();
      String sha1 = HashConverter.getSha1(datasetKey, id);
      ir.setInternalId(sha1);
    };
  }
}
