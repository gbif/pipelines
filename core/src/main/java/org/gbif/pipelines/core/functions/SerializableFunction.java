package org.gbif.pipelines.core.functions;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A function that can be used in a distributed context.
 * @param <T> The input type
 * @param <R> The return type
 */
@FunctionalInterface
public interface SerializableFunction<T,R> extends Function<T,R>, Serializable {
}
