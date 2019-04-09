package org.gbif.pipelines.core.interpreters.core;

import org.gbif.common.parsers.core.Parsable;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Optional;

/**
 * Parser for term {@link GbifTerm#typifiedName}.
 */
public class TypifiedNameInterpreter {

  private static final Parsable<String> TYPE_NAME_PARSER = org.gbif.common.parsers.TypifiedNameParser.getInstance();

  /**
   * Private constructor.
   */
  private TypifiedNameInterpreter() {
    //Do Nothing
  }

  /** {@link DwcTerm#typifiedName} interpretation. */
  public static void interpretTypifiedName(ExtendedRecord er, BasicRecord br) {
    Optional<String> typifiedName = Optional.ofNullable(er.getCoreTerms().get(GbifTerm.typifiedName.qualifiedName()));
    if (typifiedName.isPresent()) {
      br.setTypifiedName(typifiedName.get());
    } else {
      Optional.ofNullable(er.getCoreTerms().get(DwcTerm.typeStatus.qualifiedName()))
          .ifPresent(typeStatusValue -> {
            ParseResult<String> result = TYPE_NAME_PARSER.parse(er.getCoreTerms().get(DwcTerm.typeStatus.qualifiedName()));
            if (result.isSuccessful()) {
              br.setTypifiedName(result.getPayload());
            }
          });
    }

  }
}
