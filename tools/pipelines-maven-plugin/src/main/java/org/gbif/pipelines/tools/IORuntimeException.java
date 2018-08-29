package org.gbif.pipelines.tools;

class IORuntimeException extends RuntimeException {

  IORuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
