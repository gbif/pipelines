package org.gbif.pipelines.core.interpreter;

import org.apache.avro.specific.SpecificRecord;

/**
 * Adapts a T object into a K object that extends {@link SpecificRecord}.
 */
public interface RecordAdapter<T, K extends SpecificRecord> {

  K adapt(T source);

}
