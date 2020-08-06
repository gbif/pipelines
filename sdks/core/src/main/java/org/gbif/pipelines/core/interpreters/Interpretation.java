package org.gbif.pipelines.core.interpreters;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * The class is designed to simplify interpretation process:
 *
 * <pre>
 *   1) Sets source data object {@link Interpretation#from}
 *   2) Sets target data object {@link Interpretation#to}
 *   3) Uses interpretation function {@link Handler#via}
 *   4) Consumes {@link Handler#consume} or get {@link Handler#getOfNullable} the result
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
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Interpretation<S> {

  private final S source;

  /** @param source source data object */
  public static <S> Interpretation<S> from(S source) {
    return new Interpretation<>(source);
  }

  /** @param func Supplier produces source data object */
  public static <S> Interpretation<S> from(Supplier<S> func) {
    return new Interpretation<>(func.get());
  }

  /** @param target target data object */
  public <T> Condition<T> to(T target) {
    return new Condition<>(target);
  }

  /** @param func Function converts source data object to target data object */
  public <T> Condition<T> to(Function<S, T> func) {
    return new Condition<>(func.apply(source));
  }

  /** @param func Supplier produces target data object */
  public <T> Condition<T> to(Supplier<T> func) {
    return new Condition<>(func.get());
  }

  public class Condition<T> {

    private final T target;

    private Condition(T target) {
      this.target = target;
    }

    public Condition<T> when(Predicate<S> predicate) {
      return target != null && predicate.test(source) ? this : new Condition<>(null);
    }

    public Handler<T> via(BiConsumer<S, T> func) {
      return new Handler<>(target).via(func);
    }

    public Handler<T> via(Consumer<T> func) {
      return new Handler<>(target).via(func);
    }
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public class Handler<T> {

    private final T target;

    /**
     * @param func BiConsumer for applying an interpretation function, where S as a source data
     *     object and T as a target data object
     */
    public Handler<T> via(BiConsumer<S, T> func) {
      Optional.ofNullable(target).ifPresent(t -> func.accept(source, t));
      return this;
    }

    /**
     * @param func Consumer for applying an interpretation function, where T as a source data object
     *     and as a target data object
     */
    public Handler<T> via(Consumer<T> func) {
      Optional.ofNullable(target).ifPresent(func);
      return this;
    }

    /** @return target data object */
    public Optional<T> getOfNullable() {
      return Optional.ofNullable(target);
    }

    /** @return target data object */
    public Optional<T> get() {
      return Optional.of(target);
    }

    /** @param consumer Consumer for consuming target data object */
    public void consume(Consumer<T> consumer) {
      Optional.ofNullable(target).ifPresent(consumer);
    }
  }
}
