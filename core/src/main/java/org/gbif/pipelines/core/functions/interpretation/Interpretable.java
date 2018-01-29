package org.gbif.pipelines.core.functions.interpretation;

/**
 * interpret interface for various term based interpreter
 * throw InterpretationException in case found issues or lineages
 * T stands for Input parameter and U stands for output parameter
 */
public interface Interpretable<T, U> {

  public U interpret(T input) throws InterpretationException;
}
