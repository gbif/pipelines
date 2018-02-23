package org.gbif.pipelines.interpretation.column;

import java.util.function.Function;

/**
 * interpret interface for various term based interpreter
 * throw InterpretationException in case found issues or lineages
 * T stands for Input parameter and U stands for output parameter
 */
@FunctionalInterface
interface Interpretable<T, U> extends Function<T, InterpretationResult<U>> {

}
