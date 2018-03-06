package org.gbif.pipelines.labs.interpretation.lineage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

public class Lineage<T> {

  private Collection<Trace> trace;

  private T element;

  private Lineage(T element) {
    trace = new ArrayList<>();
    this.element = element;
  }

  public static <U>  Lineage<U> of(U element) {
    return new Lineage<>(element);
  }

  public Lineage<T> trace(Trace trace) {
    this.trace.add(trace);
    return this;
  }

  public Lineage<T> trace(String trace) {
    this.trace.add(Trace.info(trace));
    return this;
  }

  public <U> Lineage<U> bind(Function<T,Lineage<U>> mapper) {
    Lineage<U> lineage = mapper.apply(element);
    lineage.trace.addAll(trace);
    return lineage;
  }

}
