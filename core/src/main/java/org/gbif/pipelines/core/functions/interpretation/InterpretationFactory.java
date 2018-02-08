package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.dwc.terms.DwcTerm;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for interpretation of raw records
 */
public class InterpretationFactory {

  private static final Map<DwcTerm, Interpretable> termInterpretationMap = createMap();

  private static Map<DwcTerm, Interpretable> createMap() {
    Map<DwcTerm, Interpretable> termInterpretationMap = new HashMap<>();
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
    if (termInterpretationMap.containsKey(term)) {
      return InterpretationFactory.interpret((Interpretable<T>) termInterpretationMap.get(term), input);
    } else {
      throw new UnsupportedOperationException("Interpreter for the " + term.name() + " is not supported");
    }
  }

}
