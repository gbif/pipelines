package org.gbif.pipelines.core;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** TODO: DOC */
public class Interpretation<S> {

  private final S source;

  private Interpretation(S source) {
    this.source = source;
  }

  public static <S> Interpretation<S> from(S source) {
    return new Interpretation<>(source);
  }

  public <T> Handler<T> to(T t) {
    return new Handler<>(t);
  }

  public <T> Handler<T> to(Function<S, T> func) {
    return new Handler<>(func.apply(source));
  }

  public <T> Handler<T> to(Supplier<T> func) {
    return new Handler<>(func.get());
  }

  public class Handler<T> {

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
