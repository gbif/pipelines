package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.dwc.terms.DwcTerm;

import java.util.HashMap;
import java.util.Map;

public class InterpretationFactory {

  private static final Map<DwcTerm, Interpretable> termInterpretationMap = createMap();
  private static Map<DwcTerm, Interpretable> createMap()
  {
    Map<DwcTerm, Interpretable> termInterpretationMap = new HashMap<>();
    termInterpretationMap.put(DwcTerm.day,new DayInterpreter());
    termInterpretationMap.put(DwcTerm.month,new MonthInterpreter());
    termInterpretationMap.put(DwcTerm.year,new YearInterpreter());
    termInterpretationMap.put(DwcTerm.country,new CountryInterpreter());
    termInterpretationMap.put(DwcTerm.countryCode,new CountryCodeInterpreter());
    termInterpretationMap.put(DwcTerm.continent,new ContinentInterpreter());
    return termInterpretationMap;
  }

  public static <U,T> U interpret(Interpretable<T> interpretable,T input) throws InterpretationException {
    return interpretable.<U>interpret(input);
  }

  public static <U,T> U interpret(DwcTerm term,T input) throws InterpretationException {
    if(termInterpretationMap.containsKey(term)) return InterpretationFactory.<U, T>interpret((Interpretable<T>) termInterpretationMap.get(term), input);
    else throw new UnsupportedOperationException("Interpreter for the "+term.name()+ " is not supported");
  }

}
