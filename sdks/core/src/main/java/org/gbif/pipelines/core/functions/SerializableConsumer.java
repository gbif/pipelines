package org.gbif.pipelines.core.functions;

import java.io.Serializable;
import java.util.function.Consumer;

@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

  @Override
  void accept(T t);
}
