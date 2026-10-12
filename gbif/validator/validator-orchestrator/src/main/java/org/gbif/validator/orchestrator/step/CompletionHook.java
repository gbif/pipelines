package org.gbif.validator.orchestrator.step;

import org.gbif.validator.api.Validation;

/** A step-specific side effect executed before a completion is committed. */
@FunctionalInterface
public interface CompletionHook<C> {

  void execute(Validation validation, C completion) throws Exception;
}
