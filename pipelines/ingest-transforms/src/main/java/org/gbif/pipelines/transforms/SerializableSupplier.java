package org.gbif.pipelines.transforms;

import java.io.Serializable;
import java.util.function.Supplier;

@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {

  @Override
  T get();
}
