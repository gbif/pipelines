package org.gbif.pipelines.interpretation;

import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * This class contains TupleTag definitions to be reused on different pipelines and transformations.
 */
public class InterpretationTupleTags {

  /**
   * Utility class.
   */
  private InterpretationTupleTags() {
    //
  }

  /**
   * TupleTag of a Key Value pair of String -> InterpretedExtendedRecord.
   */
  public static TupleTag<KV<String,InterpretedExtendedRecord>> interpretedExtendedRecordKV() {
    return new TupleTag<KV<String,InterpretedExtendedRecord>>(){};
  }

  /**
   * TupleTag of OccurrenceIssue.
   */
  public static TupleTag<OccurrenceIssue> occurrenceIssueTupleTag() {
    return new TupleTag<OccurrenceIssue>(){};
  }

  /**
   * TupleTag of InterpretedExtendedRecord.
   */

  public static TupleTag<InterpretedExtendedRecord> interpretedExtendedRecordTupleTag() {
    return new TupleTag<InterpretedExtendedRecord>(){};
  }

}
