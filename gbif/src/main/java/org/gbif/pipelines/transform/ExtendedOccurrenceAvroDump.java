package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.pipelines.core.config.RecordInterpretation;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;

import java.util.Map;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionTuple;

/**
 * Transform to write final flat interpreted occurrence and issues/lineages
 */
public class ExtendedOccurrenceAvroDump extends PTransform<PCollectionTuple, PCollectionTuple> {

  private final ExtendedOccurrenceTransform transformer;
  private final Map<RecordInterpretation, String> targetPaths;

  public ExtendedOccurrenceAvroDump(ExtendedOccurrenceTransform transform, Map<RecordInterpretation, String> targetPaths) {
    transformer = transform;
    this.targetPaths = targetPaths;
  }

  @Override
  public PCollectionTuple expand(PCollectionTuple interpretedRecords) {
    // Write the big flat final interpreted records as an Avro file in defined hive table
    interpretedRecords.get(transformer.getInterpretedOccurrence())
      .apply("Save the interpreted records as Avro",
             AvroIO.write(ExtendedOccurrence.class).to(targetPaths.get(RecordInterpretation.INTERPRETED_OCURENCE)));
    // Write the issue and lineage result as an Avro file in defined table
    interpretedRecords.get(transformer.getInterpretedIssue())
      .apply("Save the interpreted records issues and lineages as Avro",
             AvroIO.write(IssueLineageRecord.class).to(targetPaths.get(RecordInterpretation.INTERPRETED_ISSUE)));
    return PCollectionTuple.empty(interpretedRecords.getPipeline());
  }
}
