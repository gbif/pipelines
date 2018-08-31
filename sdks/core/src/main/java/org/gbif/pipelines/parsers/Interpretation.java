package org.gbif.pipelines.parsers;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class Interpretation<S extends Serializable> {

  private final S source;

  private Interpretation(S source) {
    this.source = source;
  }

  public static <S extends Serializable> Interpretation<S> from(S source) {
    return new Interpretation<>(source);
  }

  public <T extends Serializable> Handler<T> to(Function<S, T> func) {
    return new Handler<>(func.apply(source));
  }

  public <T extends Serializable> Handler<T> to(Supplier<T> func) {
    return new Handler<>(func.get());
  }

  public class Handler<T extends Serializable> {

    private final T target;

    private Handler(T target) {
      this.target = target;
    }

    public Handler<T> via(BiConsumer<S, T> func) {
      func.accept(source, target);
      return this;
    }

    public Handler<T> via(Consumer<T> func) {
      func.accept(target);
      return this;
    }

    public T get() {
      return target;
    }

    public void consume(Consumer<T> consumer) {
      consumer.accept(target);
    }
  }
}
