package org.gbif.pipelines.transform.function;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.transform.ExtendedOccurrenceTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;

/**
 * Convert's Beam's represented Joined Issues PCollection to an IssueAndLineageRecord
 */
public class InterpretedIssueRecordTransform extends DoFn<KV<String, CoGbkResult>, IssueLineageRecord> {

  private final ExtendedOccurrenceTransform extendedOccurrenceTransform;

  public InterpretedIssueRecordTransform(ExtendedOccurrenceTransform extendedOccurrenceTransform) {
    this.extendedOccurrenceTransform = extendedOccurrenceTransform;
  }

  @DoFn.ProcessElement
  public void processElement(ProcessContext ctx) {
    KV<String, CoGbkResult> result = ctx.element();
    //get temporal and spatial issues info from the joined beam collection with tags
    IssueLineageRecord event = result.getValue().getOnly(extendedOccurrenceTransform.getTemporalIssueTag());
    IssueLineageRecord location = result.getValue().getOnly(extendedOccurrenceTransform.getSpatialIssueTag());

    Map<String, List<Issue>> fieldIssueMap = new HashMap<>();
    fieldIssueMap.putAll(event.getFieldIssueMap());
    fieldIssueMap.putAll(location.getFieldIssueMap());


    Map<String, List<Lineage>> fieldLineageMap = new HashMap<>();
    fieldLineageMap.putAll(event.getFieldLineageMap());
    fieldLineageMap.putAll(location.getFieldLineageMap());
    //construct a final IssueLineageRecord for all categories
    IssueLineageRecord record = IssueLineageRecord.newBuilder()
      .setOccurenceId(event.getOccurenceId())
      .setFieldIssueMap(fieldIssueMap)
      .setFieldLineageMap(fieldLineageMap)
      .build();
    ctx.output(record);
  }
}
