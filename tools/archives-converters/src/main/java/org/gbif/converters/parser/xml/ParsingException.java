package org.gbif.converters.parser.xml;

/** Thrown when there are any problems parsing content that we couldn't recover from. */
public class ParsingException extends RuntimeException {

  private static final long serialVersionUID = -8057678047764064262L;

  public ParsingException() {}

  public ParsingException(String message) {
    super(message);
  }

  public ParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParsingException(Throwable cause) {
    super(cause);
  }
}
