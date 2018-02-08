package org.gbif.pipelines.core.adapter;

import org.apache.avro.specific.SpecificRecord;

/**
 * Adapts a T object into a K object that extends {@link SpecificRecord}.
 */
@FunctionalInterface
public interface RecordAdapter<T, K extends SpecificRecord> {

  K adapt(T source);

}
