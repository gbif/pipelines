package org.gbif.pipelines.transforms.function;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.transforms.ExtendedOccurenceTransform;

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

  private final ExtendedOccurenceTransform extendedOccurenceTransform;

  public InterpretedIssueRecordTransform(ExtendedOccurenceTransform extendedOccurenceTransform) {
    this.extendedOccurenceTransform = extendedOccurenceTransform;
  }

  @DoFn.ProcessElement
  public void processElement(ProcessContext ctx) {
    KV<String, CoGbkResult> result = ctx.element();
    //get temporal and spatial issues info from the joined beam collection with tags
    IssueLineageRecord evt = result.getValue().getOnly(extendedOccurenceTransform.getTemporalIssueTag());
    IssueLineageRecord loc = result.getValue().getOnly(extendedOccurenceTransform.getSpatialIssueTag());

    Map<String, List<Issue>> fieldIssueMap = new HashMap<>();
    fieldIssueMap.putAll(evt.getFieldIssueMap());
    fieldIssueMap.putAll(loc.getFieldIssueMap());

    Map<String, List<Lineage>> fieldLineageMap = new HashMap<>();
    fieldLineageMap.putAll(evt.getFieldLineageMap());
    fieldLineageMap.putAll(loc.getFieldLineageMap());
    //construct a final IssueLineageRecord for all categories
    IssueLineageRecord record = IssueLineageRecord.newBuilder()
      .setOccurenceId(evt.getOccurenceId())
      .setFieldIssueMap(fieldIssueMap)
      .setFieldLineageMap(fieldLineageMap)
      .build();
    ctx.output(record);
  }
}
