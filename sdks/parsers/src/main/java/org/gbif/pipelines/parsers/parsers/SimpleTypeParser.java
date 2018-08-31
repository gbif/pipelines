package org.gbif.pipelines.parsers.parsers;

import org.gbif.common.parsers.BooleanParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

/** Utility class that parses basic data types. */
public class SimpleTypeParser {

  // Caching instance of BooleanParser since it is a file based parser
  private static final BooleanParser BOOLEAN_PARSER = BooleanParser.getInstance();

  /** Utility class. */
  private SimpleTypeParser() {
    // hidden constructor
  }

  /** Parses an integer value and consumes its response (if any). */
  public static void parseInt(
      ExtendedRecord er, DwcTerm term, Consumer<Optional<Integer>> consumer) {
    Optional.ofNullable(extractValue(er, term))
        .ifPresent(
            termValue ->
                consumer.accept(Optional.ofNullable(NumberParser.parseInteger(termValue))));
  }

  /** Parses an integer value and applies a mapping function to its response (if any). */
  public static <U> Optional<U> parseInt(
      ExtendedRecord er, DwcTerm term, Function<Optional<Integer>, U> mapper) {
    return Optional.ofNullable(extractValue(er, term))
        .map(termValue -> mapper.apply(Optional.ofNullable(NumberParser.parseInteger(termValue))));
  }

  /** Parses a double value and consumes its response (if any). */
  public static void parseDouble(
      ExtendedRecord er, DwcTerm term, Consumer<Optional<Double>> consumer) {
    Optional.ofNullable(extractValue(er, term))
        .ifPresent(
            termValue -> consumer.accept(Optional.ofNullable(NumberParser.parseDouble(termValue))));
  }

  /** Parses a double value and applies a mapping function to its response (if any). */
  public static <U> Optional<U> parseDouble(
      ExtendedRecord er, DwcTerm term, Function<Optional<Double>, U> mapper) {
    return Optional.ofNullable(extractValue(er, term))
        .map(termValue -> mapper.apply(Optional.ofNullable(NumberParser.parseDouble(termValue))));
  }

  /** Parses a boolean value and consumes its response (if any). */
  public static void parseBoolean(
      ExtendedRecord er, DwcTerm term, Consumer<ParseResult<Boolean>> consumer) {
    Optional.ofNullable(extractValue(er, term))
        .ifPresent(termValue -> consumer.accept(BOOLEAN_PARSER.parse(termValue)));
  }

  /** Parses a boolean value and applies mapping functions to its response (if any). */
  public static <U> Optional<U> parseBoolean(
      ExtendedRecord er, DwcTerm term, Function<ParseResult<Boolean>, U> mapper) {
    return Optional.ofNullable(extractValue(er, term))
        .map(termValue -> mapper.apply(BOOLEAN_PARSER.parse(termValue)));
  }
}
