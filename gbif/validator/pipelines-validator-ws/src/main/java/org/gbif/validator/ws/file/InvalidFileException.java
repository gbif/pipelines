package org.gbif.validator.ws.file;

/** A submitted file the validator can't process. Its message is shown to users. */
public class InvalidFileException extends IllegalArgumentException {
  public InvalidFileException(String message) {
    super(message);
  }
}
