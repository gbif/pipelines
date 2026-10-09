package org.gbif.validator.orchestrator.service;

/**
 * Unchecked wrapper for failures crossing callback interfaces without checked exceptions.
 *
 * <p>postal-api's MessageConsumer catches this exception and negatively acknowledges the delivery.
 * Its current negative acknowledgement policy does not requeue the message.
 */
public class OrchestrationProcessingException extends RuntimeException {

  public OrchestrationProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
