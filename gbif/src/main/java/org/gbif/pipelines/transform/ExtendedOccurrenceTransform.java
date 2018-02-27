package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.mapper.ExtendedOccurrenceMapper;

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
public class ExtendedOccurrenceTransform extends RecordTransform<ExtendedRecord, ExtendedOccurrence> {

  private static final String DATA_STEP_NAME = "Interpret ExtendedOccurrence record";
  private static final String ISSUE_STEP_NAME = "Interpret ExtendedOccurrence issue";

  // Data tupple tags for internal usage only
  private final TupleTag<InterpretedExtendedRecord> recordDataTag = new TupleTag<InterpretedExtendedRecord>() {};
  private final TupleTag<Location> locationDataTag = new TupleTag<Location>() {};
  private final TupleTag<TemporalRecord> temporalDataTag = new TupleTag<TemporalRecord>() {};
  // Issue tupple tags for internal usage only
  private final TupleTag<IssueLineageRecord> recordIssueTag = new TupleTag<IssueLineageRecord>() {};
  private final TupleTag<IssueLineageRecord> locationIssueTag = new TupleTag<IssueLineageRecord>() {};
  private final TupleTag<IssueLineageRecord> temporalIssueTag = new TupleTag<IssueLineageRecord>() {};

  public ExtendedOccurrenceTransform() {
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

    // Map ExtendedOccurrence records
    PCollection<KV<String, ExtendedOccurrence>> occurrenceCollection = groupedData.apply(DATA_STEP_NAME, mapOccurrenceParDo());

    // Map ExtendedOccurrence issues
    PCollection<KV<String, IssueLineageRecord>> issueCollection = groupedIssue.apply(ISSUE_STEP_NAME, mapIssueParDo());

    // Return data and issue
    return PCollectionTuple.of(getDataTag(), occurrenceCollection).and(getIssueTag(), issueCollection);
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
          .setOccurrenceId(record.getOccurrenceId())
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
  private ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, ExtendedOccurrence>> mapOccurrenceParDo() {
    return ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, ExtendedOccurrence>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> element = c.element();

        CoGbkResult value = element.getValue();

        InterpretedExtendedRecord record = value.getOnly(recordDataTag);
        Location location = value.getOnly(locationDataTag);
        TemporalRecord temporal = value.getOnly(temporalDataTag);

        ExtendedOccurrence occurrence = ExtendedOccurrenceMapper.map(record, location, temporal);

        c.output(KV.of(element.getKey(), occurrence));
      }
    });
  }

  @Override
  DoFn<ExtendedRecord, KV<String, ExtendedOccurrence>> interpret() {
    throw new UnsupportedOperationException("The method is not implemented");
  }

}
