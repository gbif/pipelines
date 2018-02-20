package org.gbif.pipelines.interpretation.column;

import org.gbif.dwc.terms.DwcTerm;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * Helper class for interpretation of raw records
 */
public class InterpretationFactory {

    private static final Map<DwcTerm, Interpretable> TERM_INTERPRETATION_MAP = new EnumMap<>(DwcTerm.class);

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
    public static <U, T> InterpretationResult<U> interpret(Interpretable<T,U> interpretable, T input) {
        if (input == null) return InterpretationResult.withSuccess(null);
        return interpretable.apply(input);
    }

    /**
     * returns InterpretedResult if the interpreter is available else throw UnsupportedOperationException
     */
    public static <U, T> InterpretationResult<U> interpret(DwcTerm term, T input) {
       return interpret(Optional.ofNullable(TERM_INTERPRETATION_MAP.get(term)).orElseThrow(() -> new UnsupportedOperationException("Interpreter for the " + term.name() + " is not supported")),input);
    }

}
