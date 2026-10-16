package org.gbif.validator.orchestrator.step;

/** Pure transformation of a message at a job boundary. */
@FunctionalInterface
public interface MessageAdapter<I, O> {

  O adapt(I message, JobContext context) throws Exception;
}
