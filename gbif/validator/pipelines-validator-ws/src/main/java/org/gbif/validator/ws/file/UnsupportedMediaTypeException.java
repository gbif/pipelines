package org.gbif.validator.ws.file;

/** Media not supported exception. */
public class UnsupportedMediaTypeException extends Exception {
  public UnsupportedMediaTypeException(String msg) {
    super(msg);
  }
}
