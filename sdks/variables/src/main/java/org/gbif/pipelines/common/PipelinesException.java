package org.gbif.pipelines.common;

import lombok.Getter;

@Getter
public class PipelinesException extends RuntimeException {

  private static final long serialVersionUID = -7034897190745766789L;

  private final String shortMessage;

  public PipelinesException(String var1) {
    super(var1);
    this.shortMessage = var1;
  }

  public PipelinesException(String var1, Throwable var2) {
    super(var1, var2);
    this.shortMessage = var1;
  }

  public PipelinesException(Throwable var1) {
    super(var1);
    this.shortMessage = var1.getMessage();
  }
}
