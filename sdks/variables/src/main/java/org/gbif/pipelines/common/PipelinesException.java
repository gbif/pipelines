package org.gbif.pipelines.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesException extends RuntimeException {

  static final long serialVersionUID = -7034897190745766789L;

  public PipelinesException(String var1) {
    super(var1);
  }

  public PipelinesException(String var1, Throwable var2) {
    super(var1, var2);
  }

  public PipelinesException(Throwable var1) {
    super(var1);
  }
}
