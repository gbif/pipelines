package org.gbif.pipelines.core.interpreter.taxonomy;

/**
 * Exception to use in the interpretation of taxonomic fields.
 */
public class TaxonomyInterpretationException extends Exception {

  private static final long serialVersionUID = 2798432399982522179L;

  public TaxonomyInterpretationException(String message) {
    super(message);
  }

  public TaxonomyInterpretationException(String message, Throwable thr) {
    super(message, thr);
  }

}
