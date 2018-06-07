
package org.gbif.pipelines.labs.transform;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.io.avro.ExtendedOccurrence;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.issue.Validation;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.labs.mapper.ExtendedOccurrenceMapper;
import org.gbif.pipelines.transform.RecordTransform;
import org.gbif.pipelines.transform.record.InterpretedExtendedRecordTransform;
import org.gbif.pipelines.transform.record.LocationRecordTransform;
import org.gbif.pipelines.transform.record.TemporalRecordTransform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 *
 */
public class ExtendedOccurrenceTransform extends RecordTransform<ExtendedRecord, ExtendedOccurrence> {

  private static final String DATA_STEP_NAME = "Interpret ExtendedOccurrence record";

  // Data tupple tags for internal usage only
  private final TupleTag<InterpretedExtendedRecord> recordDataTag = new TupleTag<InterpretedExtendedRecord>() {};
  private final TupleTag<LocationRecord> locationDataTag = new TupleTag<LocationRecord>() {};
  private final TupleTag<TemporalRecord> temporalDataTag = new TupleTag<TemporalRecord>() {};
  // Issue tupple tags for internal usage only
  private final TupleTag<OccurrenceIssue> recordIssueTag = new TupleTag<OccurrenceIssue>() {};
  private final TupleTag<OccurrenceIssue> locationIssueTag = new TupleTag<OccurrenceIssue>() {};
  private final TupleTag<OccurrenceIssue> temporalIssueTag = new TupleTag<OccurrenceIssue>() {};

  private ExtendedOccurrenceTransform() {
    super(DATA_STEP_NAME);
  }

  public static ExtendedOccurrenceTransform create(){
    return new ExtendedOccurrenceTransform();
  }

  /**
   *
   */
  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {

    // STEP 1: Collect all records
    // Collect ExtendedRecord
    InterpretedExtendedRecordTransform recordTransform = InterpretedExtendedRecordTransform.create();
    PCollectionTuple recordTupple = input.apply(recordTransform);

    // Collect Location
    LocationRecordTransform locationTransform = LocationRecordTransform.create();
    PCollectionTuple locationTuple = input.apply(locationTransform);

    // Collect TemporalRecord
    TemporalRecordTransform temporalTransform = TemporalRecordTransform.create();
    PCollectionTuple temporalTupple = input.apply(temporalTransform);

    // STEP 2: Group records and issues by key
    // Group records collections
    PCollection<KV<String, CoGbkResult>> groupedData =
      KeyedPCollectionTuple.of(recordDataTag, recordTupple.get(recordTransform.getDataTag()))
        .and(locationDataTag, locationTuple.get(locationTransform.getDataTag()))
        .and(temporalDataTag, temporalTupple.get(temporalTransform.getDataTag()))
        .and(recordIssueTag, recordTupple.get(recordTransform.getIssueTag()))
        .and(locationIssueTag, locationTuple.get(locationTransform.getIssueTag()))
        .and(temporalIssueTag, temporalTupple.get(temporalTransform.getIssueTag()))
        .apply(CoGroupByKey.create());

    // Map ExtendedOccurrence records and issues
    return groupedData.apply(DATA_STEP_NAME, proccessGroupDoFn());
  }

  private ParDo.MultiOutput<KV<String, CoGbkResult>, KV<String, ExtendedOccurrence>> proccessGroupDoFn() {
    return ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, ExtendedOccurrence>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> element = c.element();
        String key = element.getKey();

        // Map data
        ExtendedOccurrence occurrence = mapToExtendedOccurrence(element);

        // Map issues
        OccurrenceIssue issue = mapToOccurrenceIssue(element);

        // Output
        c.output(getDataTag(), KV.of(key, occurrence));
        c.output(getIssueTag(), KV.of(key, issue));
      }
    }).withOutputTags(getDataTag(), TupleTagList.of(getIssueTag()));
  }

  /**
   * Group data
   */
  private ExtendedOccurrence mapToExtendedOccurrence(KV<String, CoGbkResult> element) {
    CoGbkResult value = element.getValue();

    InterpretedExtendedRecord record = value.getOnly(recordDataTag);
    LocationRecord location = value.getOnly(locationDataTag);
    TemporalRecord temporal = value.getOnly(temporalDataTag);
    TaxonRecord taxon = TaxonRecord.newBuilder().setId(record.getId()).build();
    MultimediaRecord multimedia = MultimediaRecord.newBuilder().setId(record.getId()).build();

    return ExtendedOccurrenceMapper.map(record, location, temporal, taxon, multimedia);
  }

  /**
   * Group issues
   */
  private OccurrenceIssue mapToOccurrenceIssue(KV<String, CoGbkResult> element) {
    CoGbkResult value = element.getValue();
    String key = element.getKey();

    // default values
    List<Validation> defaultValidationList = Collections.emptyList();
    OccurrenceIssue defaultIssue = OccurrenceIssue.newBuilder().setId(key).setIssues(defaultValidationList).build();

    OccurrenceIssue recordIssue = value.getOnly(recordIssueTag, defaultIssue);
    OccurrenceIssue locationIssue = value.getOnly(locationIssueTag, defaultIssue);
    OccurrenceIssue temporalIssue = value.getOnly(temporalIssueTag, defaultIssue);

    List<Validation> recordIssueList = cast(recordIssue.getIssues());
    List<Validation> locationIssueList = cast(locationIssue.getIssues());
    List<Validation> temporalIssueList = cast(temporalIssue.getIssues());

    int size = recordIssueList.size() + locationIssueList.size() + temporalIssueList.size();
    if (size > 0) {
      List<Validation> validations = new ArrayList<>(size);
      validations.addAll(recordIssueList);
      validations.addAll(locationIssueList);
      validations.addAll(temporalIssueList);

      return OccurrenceIssue.newBuilder().setId(key).setIssues(validations).build();
    }
    return defaultIssue;
  }

  @Override
  public DoFn<ExtendedRecord, KV<String, ExtendedOccurrence>> interpret() {
    throw new UnsupportedOperationException("The method is not implemented");
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(final Object o) {
    return (T) o;
  }

  @Override
  public ExtendedOccurrenceTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class, ExtendedOccurrence.class, OccurrenceIssue.class);
    Coders.registerAvroCoders(pipeline, InterpretedExtendedRecord.class, TemporalRecord.class, LocationRecord.class);
    return this;
  }

}
