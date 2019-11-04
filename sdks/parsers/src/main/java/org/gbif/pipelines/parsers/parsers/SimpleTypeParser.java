package org.gbif.pipelines.parsers.parsers;

import java.util.Optional;
import java.util.function.Consumer;

import org.gbif.common.parsers.NumberParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.parsers.utils.ModelUtils.extractNullAwareValue;

/** Utility class that parses basic data types. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SimpleTypeParser {

  /** Parses an integer value and consumes its response (if any). */
  public static void parseInt(ExtendedRecord er, DwcTerm term, Consumer<Optional<Integer>> consumer) {
    Optional.ofNullable(extractNullAwareValue(er, term))
        .ifPresent(termValue -> consumer.accept(Optional.ofNullable(NumberParser.parseInteger(termValue))));
  }

  /** Parses a double value and consumes its response (if any). */
  public static void parseDouble(ExtendedRecord er, DwcTerm term, Consumer<Optional<Double>> consumer) {
    parseDouble(extractNullAwareValue(er, term), consumer);
  }

  /** Parses a double value and consumes its response (if any). */
  public static void parseDouble(String value, Consumer<Optional<Double>> consumer) {
    Optional.ofNullable(value)
        .ifPresent(termValue -> consumer.accept(Optional.ofNullable(NumberParser.parseDouble(termValue))));
  }
}
