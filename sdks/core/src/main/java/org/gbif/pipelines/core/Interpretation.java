package org.gbif.pipelines.core;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The class is designed to simplify interpretation process:
 *
 * <pre>
 *   1) Sets source data object {@link Interpretation#from}
 *   2) Sets target data object {@link Interpretation#to}
 *   3) Uses interpretation function {@link Handler#via}
 *   4) Consumes {@link Handler#consume} or get {@link Handler#get} the result
 * </pre>
 *
 * <p>Example:
 *
 * <pre>{@code
 * Interpretation.from(context::element)
 *     .to(er -> TemporalRecord.newBuilder().setId(er.getId()).build())
 *     .via(TemporalInterpreter::interpretEventDate)
 *     .via(TemporalInterpreter::interpretDateIdentified)
 *     .via(TemporalInterpreter::interpretModifiedDate)
 *     .via(TemporalInterpreter::interpretDayOfYear)
 *     .consume(context::output);
 * }</pre>
 */
public class Interpretation<S> {

  private final S source;

  private Interpretation(S source) {
    this.source = source;
  }

  /** @param source source data object */
  public static <S> Interpretation<S> from(S source) {
    return new Interpretation<>(source);
  }

  /** @param func Supplier produces source data object */
  public static <S> Interpretation<S> from(Supplier<S> func) {
    return new Interpretation<>(func.get());
  }

  /** @param target target data object */
  public <T> Handler<T> to(T target) {
    return new Handler<>(target);
  }

  /** @param func Function converts source data object to target data object */
  public <T> Handler<T> to(Function<S, T> func) {
    return new Handler<>(func.apply(source));
  }

  /** @param func Supplier produces target data object */
  public <T> Handler<T> to(Supplier<T> func) {
    return new Handler<>(func.get());
  }

  public class Handler<T> {

    private final T target;

    private Handler(T target) {
      this.target = target;
    }

    /**
     * @param func BiConsumer for applying an interpretation function, where S as a source data
     *     object and T as a target data object
     */
    public Handler<T> via(BiConsumer<S, T> func) {
      func.accept(source, target);
      return this;
    }

    /**
     * @param func Consumer for applying an interpretation function, where T as a source data object
     *     and as a target data object
     */
    public Handler<T> via(Consumer<T> func) {
      func.accept(target);
      return this;
    }

    /** @return target data object */
    public T get() {
      return target;
    }

    /** @param consumer Consumer for consuming target data object */
    public void consume(Consumer<T> consumer) {
      consumer.accept(target);
    }
  }
}
