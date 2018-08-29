package org.gbif.pipelines.core;

import java.io.Serializable;
import java.util.Objects;

public class Context<S extends Serializable, T extends Serializable> implements Serializable {

  private static final long serialVersionUID = -5417891544515985212L;

  private final S source;
  private final T target;

  public Context(S source, T target) {
    this.source = source;
    this.target = target;
  }

  public S getSource() {
    return source;
  }

  public T getTarget() {
    return target;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Context<?, ?> box = (Context<?, ?>) o;
    return Objects.equals(source, box.source) && Objects.equals(target, box.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, target);
  }
}
