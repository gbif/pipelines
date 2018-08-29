package org.gbif.pipelines.core.utils;

public class IORuntimeException extends RuntimeException {

  public IORuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
