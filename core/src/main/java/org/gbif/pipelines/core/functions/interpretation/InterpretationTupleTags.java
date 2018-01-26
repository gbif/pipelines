package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.io.avro.ExtendedRecord;
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

  public static final TupleTag<ExtendedRecord> EXTENDED_RECORD_TUPLE_TAG = new TupleTag<ExtendedRecord>(){};

  public static final TupleTag<InterpretedExtendedRecord> INTERPRETED_EXTENDED_RECORD_TUPLE_TAG = new TupleTag<InterpretedExtendedRecord>(){};

  public static final TupleTag<OccurrenceIssue> OCCURRENCE_ISSUE_TUPLE_TAG = new TupleTag<OccurrenceIssue>(){};


  public static TupleTag<KV<String,InterpretedExtendedRecord>> interpretedExtendedRecordKV() {
    return new TupleTag<KV<String,InterpretedExtendedRecord>>(){};
  }

}
