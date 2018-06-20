package org.gbif.pipelines.transform.functions;

import org.gbif.dwc.record.StarRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

/** Provider of reusable functions. All functions implement serializable. */
public class FunctionFactory {

  private FunctionFactory() {}

  public static Function<StarRecord, ExtendedRecord> extendedRecordBuilder() {
    return new ExtendedRecordBuilder();
  }
}
