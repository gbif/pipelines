package org.gbif.pipelines.core;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class Interpretation<S extends Serializable> {

  private final S source;

  private Interpretation(S source) {
    this.source = source;
  }

  public static <S extends Serializable> Interpretation<S> of(S source) {
    return new Interpretation<>(source);
  }

  public <T extends Serializable> Handler<S, T> convert(Function<S, Context<S, T>> func) {
    return new Handler<>(func.apply(source));
  }

  public static class Handler<S extends Serializable, T extends Serializable> {
    private final Context<S, T> context;

    private Handler(Context<S, T> context) {
      this.context = context;
    }

    public Handler<S, T> using(BiConsumer<S, T> func) {
      func.accept(context.getSource(), context.getTarget());
      return this;
    }

    public Handler<S, T> using(Consumer<Context<S, T>> func) {
      func.accept(context);
      return this;
    }

    public T getValue() {
      return context.getTarget();
    }

    public void consumeValue(Consumer<T> consumer) {
      consumer.accept(getValue());
    }

    public Context<S, T> getContext() {
      return context;
    }
  }
}
