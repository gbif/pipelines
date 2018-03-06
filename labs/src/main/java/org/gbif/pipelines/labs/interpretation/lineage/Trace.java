package org.gbif.pipelines.labs.interpretation.lineage;

public class Trace {

  private final String message;
  private final TraceType type;

  private Trace(String message, TraceType type) {
    this.message = message;
    this.type = type;
  }

  public static Trace transformation(String msg) {
    return new Trace(msg, TraceType.TRANSFORMATION);
  }

  public static Trace error(String msg) {
    return new Trace(msg, TraceType.ERROR);
  }

  public static Trace info(String msg) {
    return new Trace(msg, TraceType.INFO);
  }

  public String getMessage() {
    return message;
  }

  public TraceType getType() {
    return type;
  }
}
