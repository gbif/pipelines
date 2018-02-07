package org.gbif.pipelines.core.functions.interpretation;

/**
 * interpret interface for various term based interpreter
 * throw InterpretationException in case found issues or lineages
 * T stands for Input parameter and U stands for output parameter
 */
@FunctionalInterface
interface Interpretable<T> {

  <U> U interpret(T input) throws InterpretationException;
}
