package org.gbif.pipelines.interpretation.parsers;


import org.gbif.common.parsers.BooleanParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Utility class that parses basic data types.
 */
public class SimpleTypeParser {

  //Caching instance of BooleanParser since it is a file based parser
  private static final BooleanParser BOOLEAN_PARSER = BooleanParser.getInstance();

  /**
   * Utility class.
   */
  private SimpleTypeParser() {
    // hidden constructor
  }

  /**
   * Parses an integer value and consumes its response (if any).
   */
  public static void parseInt(ExtendedRecord extendedRecord, DwcTerm term, Consumer<Optional<Integer>> consumer) {
    Optional
      .ofNullable(extendedRecord.getCoreTerms().get(term.qualifiedName()))
      .ifPresent(termValue -> consumer.accept(Optional.ofNullable(NumberParser.parseInteger(termValue.toString()))));

  }

  /**
   * Parses a double value and consumes its response (if any).
   */
  public static void parseDouble(ExtendedRecord extendedRecord, DwcTerm term, Consumer<Optional<Double>> consumer) {
    Optional
      .ofNullable(extendedRecord.getCoreTerms().get(term.qualifiedName()))
      .ifPresent(termValue -> consumer.accept(Optional.ofNullable(NumberParser.parseDouble(termValue.toString()))));

  }


  /**
   * Parses a boolean value and consumes its response (if any).
   */
  public static void parseBoolean(ExtendedRecord extendedRecord, DwcTerm term, Consumer<ParseResult<Boolean>> consumer) {
    Optional
      .ofNullable(extendedRecord.getCoreTerms().get(term.qualifiedName()))
      .ifPresent(termValue -> consumer.accept(BOOLEAN_PARSER.parse(termValue.toString())));

  }
}
