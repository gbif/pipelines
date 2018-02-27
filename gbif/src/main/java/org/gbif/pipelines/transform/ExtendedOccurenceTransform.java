package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.mapper.ExtendedOccurenceMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 *
 */
public class ExtendedOccurenceTransform extends RecordTransform<ExtendedRecord, ExtendedOccurence> {

  private static final String DATA_STEP_NAME = "Interpret ExtendedOccurence record";
  private static final String ISSUE_STEP_NAME = "Interpret ExtendedOccurence issue";

  // Data tupple tags
  private final TupleTag<InterpretedExtendedRecord> recordDataTag = new TupleTag<InterpretedExtendedRecord>() {};
  private final TupleTag<Location> locationDataTag = new TupleTag<Location>() {};
  private final TupleTag<TemporalRecord> temporalDataTag = new TupleTag<TemporalRecord>() {};
  // Issue tupple tags
  private final TupleTag<IssueLineageRecord> recordIssueTag = new TupleTag<IssueLineageRecord>() {};
  private final TupleTag<IssueLineageRecord> locationIssueTag = new TupleTag<IssueLineageRecord>() {};
  private final TupleTag<IssueLineageRecord> temporalIssueTag = new TupleTag<IssueLineageRecord>() {};

  public ExtendedOccurenceTransform() {
    super(DATA_STEP_NAME);
  }

  /**
   *
   */
  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {

    // Collect ExtendedRecord
    InterpretedExtendedRecordTransform recordTransform = new InterpretedExtendedRecordTransform();
    PCollectionTuple recordTupple = input.apply(recordTransform);

    // Collect Location
    LocationTransform locationTransform = new LocationTransform();
    PCollectionTuple locationTuple = input.apply(locationTransform);

    // Collect TemporalRecord
    TemporalRecordTransform temporalTransform = new TemporalRecordTransform();
    PCollectionTuple temporalTupple = input.apply(temporalTransform);

    // Group records collections
    PCollection<KV<String, CoGbkResult>> groupedData =
      KeyedPCollectionTuple.of(recordDataTag, recordTupple.get(recordTransform.getDataTag()))
        .and(locationDataTag, locationTuple.get(locationTransform.getDataTag()))
        .and(temporalDataTag, temporalTupple.get(temporalTransform.getDataTag()))
        .apply(CoGroupByKey.create());

    // Group records issue collections
    PCollection<KV<String, CoGbkResult>> groupedIssue =
      KeyedPCollectionTuple.of(recordIssueTag, recordTupple.get(recordTransform.getIssueTag()))
        .and(locationIssueTag, locationTuple.get(locationTransform.getIssueTag()))
        .and(temporalIssueTag, temporalTupple.get(temporalTransform.getIssueTag()))
        .apply(CoGroupByKey.create());

    // Map ExtendedOccurence records
    PCollection<KV<String, ExtendedOccurence>> occurenceCollection = groupedData.apply(DATA_STEP_NAME, mapOccurenceParDo());

    // Map ExtendedOccurence issues
    PCollection<KV<String, IssueLineageRecord>> issueCollection = groupedIssue.apply(ISSUE_STEP_NAME, mapIssueParDo());

    // Return data and issue
    return PCollectionTuple.of(getDataTag(), occurenceCollection).and(getIssueTag(), issueCollection);
  }

  /**
   *
   */
  private ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, IssueLineageRecord>> mapIssueParDo() {
    return ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, IssueLineageRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> element = c.element();

        CoGbkResult value = element.getValue();

        IssueLineageRecord record = value.getOnly(recordIssueTag);
        IssueLineageRecord location = value.getOnly(locationIssueTag);
        IssueLineageRecord temporal = value.getOnly(temporalIssueTag);

        Map<String, List<Issue>> fieldIssueMap = new HashMap<>();
        fieldIssueMap.putAll(record.getFieldIssueMap());
        fieldIssueMap.putAll(location.getFieldIssueMap());
        fieldIssueMap.putAll(temporal.getFieldIssueMap());

        Map<String, List<Lineage>> fieldLineageMap = new HashMap<>();
        fieldLineageMap.putAll(record.getFieldLineageMap());
        fieldLineageMap.putAll(location.getFieldLineageMap());
        fieldLineageMap.putAll(temporal.getFieldLineageMap());

        //construct a final IssueLineageRecord for all categories
        IssueLineageRecord issueLineageRecord = IssueLineageRecord.newBuilder()
          .setOccurenceId(record.getOccurenceId())
          .setFieldIssueMap(fieldIssueMap)
          .setFieldLineageMap(fieldLineageMap)
          .build();

        c.output(KV.of(element.getKey(), issueLineageRecord));
      }
    });
  }

  /**
   *
   */
  private ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, ExtendedOccurence>> mapOccurenceParDo() {
    return ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, ExtendedOccurence>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> element = c.element();

        CoGbkResult value = element.getValue();

        InterpretedExtendedRecord record = value.getOnly(recordDataTag);
        Location location = value.getOnly(locationDataTag);
        TemporalRecord temporal = value.getOnly(temporalDataTag);

        ExtendedOccurence occurence = ExtendedOccurenceMapper.map(record, location, temporal);

        c.output(KV.of(element.getKey(), occurence));
      }
    });
  }

  @Override
  DoFn<ExtendedRecord, KV<String, ExtendedOccurence>> interpret() {
    throw new UnsupportedOperationException("The method is not implemented");
  }

}
