package org.gbif.pipelines.interpretation.column;

import org.gbif.dwc.terms.DwcTerm;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class for interpretation of raw records
 */
public class InterpretationFactory {

  private InterpretationFactory() {
    // Can't have an instance
  }

  private static final Map<DwcTerm, Interpretable<String,?>> TERM_INTERPRETATION_MAP = new EnumMap<>(DwcTerm.class);

  static {
    TERM_INTERPRETATION_MAP.put(DwcTerm.day, new DayInterpreter());
    TERM_INTERPRETATION_MAP.put(DwcTerm.month, new MonthInterpreter());
    TERM_INTERPRETATION_MAP.put(DwcTerm.year, new YearInterpreter());
    TERM_INTERPRETATION_MAP.put(DwcTerm.country, new CountryInterpreter());
    TERM_INTERPRETATION_MAP.put(DwcTerm.countryCode, new CountryCodeInterpreter());
    TERM_INTERPRETATION_MAP.put(DwcTerm.continent, new ContinentInterpreter());
  }

  /**
   * use it if you have custom interpreter
   */
  public static <T,U> InterpretationResult<U> interpret(Interpretable<T,U> interpretable, T input) {
    return input == null ? InterpretationResult.withSuccess(null) : interpretable.apply(input);
  }

  /**
   * returns InterpretedResult if the interpreter is available else throw UnsupportedOperationException
   */
  public static <U> InterpretationResult<U> interpret(DwcTerm term, String input) {
    String errorText = "Interpreter for the " + term.name() + " is not supported";
    Interpretable<String,U> interpretable = (Interpretable<String,U>)Objects.requireNonNull(TERM_INTERPRETATION_MAP.get(term), errorText);
    return interpret(interpretable, input);
  }

}
