package org.gbif.pipelines.core.functions;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {

  @Override
  R apply(T t);
}
