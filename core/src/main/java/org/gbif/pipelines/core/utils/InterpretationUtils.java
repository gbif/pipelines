package org.gbif.pipelines.core.utils;

import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Optional;

public class InterpretationUtils {

  private InterpretationUtils() {
    // NOP
  }

  public static String extract(ExtendedRecord record, Term dwcTerm) {
    return Optional.ofNullable(record.getCoreTerms().get(dwcTerm.qualifiedName())).orElse("");
  }
}
