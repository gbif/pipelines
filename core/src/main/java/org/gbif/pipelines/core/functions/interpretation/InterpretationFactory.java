package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.dwc.terms.DwcTerm;

import java.util.EnumMap;
import java.util.Map;

/**
 * Helper class for interpretation of raw records
 */
public class InterpretationFactory {

  private static final Map<DwcTerm, Interpretable<String>> TERM_INTERPRETATION_MAP = createMap();

  private static Map<DwcTerm, Interpretable<String>> createMap() {
    Map<DwcTerm, Interpretable<String>> termInterpretationMap = new EnumMap<>(DwcTerm.class);
    termInterpretationMap.put(DwcTerm.day, new DayInterpreter());
    termInterpretationMap.put(DwcTerm.month, new MonthInterpreter());
    termInterpretationMap.put(DwcTerm.year, new YearInterpreter());
    termInterpretationMap.put(DwcTerm.country, new CountryInterpreter());
    termInterpretationMap.put(DwcTerm.countryCode, new CountryCodeInterpreter());
    termInterpretationMap.put(DwcTerm.continent, new ContinentInterpreter());
    return termInterpretationMap;
  }

  /**
   * use it if you have custom interpreter
   */
  public static <U, T> InterpretationResult<U> interpret(Interpretable<T> interpretable, T input) {
    if (input == null) return InterpretationResult.withSuccess(null);
    return interpretable.interpret(input);
  }

  /**
   * returns InterpretedResult if the interpreter is available else throw UnsupportedOperationException
   */
  public static <U, T> InterpretationResult<U> interpret(DwcTerm term, T input) {
    if (TERM_INTERPRETATION_MAP.containsKey(term)) {
      return interpret((Interpretable<T>) TERM_INTERPRETATION_MAP.get(term), input);
    } else {
      throw new UnsupportedOperationException("Interpreter for the " + term.name() + " is not supported");
    }
  }

}
