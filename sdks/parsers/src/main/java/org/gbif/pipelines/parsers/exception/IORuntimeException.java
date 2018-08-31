package org.gbif.pipelines.parsers.exception;

/** Base wrapper for IOException */
public class IORuntimeException extends RuntimeException {

  public IORuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public IORuntimeException(Throwable cause) {
    super(cause);
  }
}
