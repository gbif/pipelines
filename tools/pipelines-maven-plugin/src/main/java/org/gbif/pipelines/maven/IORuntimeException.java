package org.gbif.pipelines.maven;

class IORuntimeException extends RuntimeException {

  IORuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
