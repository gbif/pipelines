package org.gbif.pipelines.demo;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.transforms.DwCATermIdentifier;
import org.gbif.pipelines.core.functions.transforms.ExtendedRecordToEventTransformer;
import org.gbif.pipelines.core.functions.transforms.ExtendedRecordToLocationTransformer;
import org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;

import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.spatialCategory;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.spatialCategoryIssues;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.temporalCategory;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.temporalCategoryIssues;

public class InterpretedCategoryTransformerTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final String VALID_OCC_ID="VALID_ID";
  static final String EMPTY_OCC_ID="";
  static final String NULL_OCC_ID=null;

  static final String valid_day ="12";
  static final String valid_month="2";
  static final String valid_year="2015";

  static final String range_invalid_day ="35";
  static final String range_invalid_month="-1";
  static final String range_invalid_year="1";

  static final String null_day =null;
  static final String null_month=null;
  static final String null_year=null;

  static final String empty_day ="";
  static final String empty_month="";
  static final String empty_year="";

  static final String invalid_day ="Ja";
  static final String invalid_month="May";
  static final String invalid_year="Y2K";

  static ExtendedRecord VALID_INPUT(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(),valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  static ExtendedRecord INVALID_DAY_INVALID_INPUT1(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),range_invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(),valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  static ExtendedRecord INVALID_INPUT2_ALL(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  static ExtendedRecord INVALID_MONTH_INVALID_INPUT2(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(),invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  static ExtendedRecord INVALID_YEAR_INVALID_INPUT2(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(),valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  static ExtendedRecord INVALID_DAY_YEAR_INVALID_INPUT2(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(),valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  static ExtendedRecord INVALID_MONTH_YEAR_INVALID_INPUT2(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(),invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  static ExtendedRecord INVALID_MONTH_DAY_INVALID_INPUT2(){
    Map<CharSequence,CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(),invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(),invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(),valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder().setId(VALID_OCC_ID).setCoreTerms(coreTerms).setExtensions(new HashMap<>()).build();
  }

  public ExtendedRecord[] getExtendedRecordArray() {
    return new ExtendedRecord[]{
      INVALID_MONTH_INVALID_INPUT2(), VALID_INPUT(),
        INVALID_INPUT2_ALL(), INVALID_DAY_YEAR_INVALID_INPUT2()
    } ;

  }

  public List<KV<String,Event>> getTemporal() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord,KV<String,Event>> tester = DoFnTester.of(ertoevt);
    return  tester.processBundle(getExtendedRecordArray());
  }

  public List<KV<String,IssueLineageRecord>> getTemporalIssues() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord,KV<String,Event>> tester = DoFnTester.of(ertoevt);
    tester.processBundle(getExtendedRecordArray());
    return tester.peekOutputElements(ExtendedRecordToEventTransformer.eventIssueTag);
  }

  public List<KV<String,Location>> getSpatial() throws Exception {
    ExtendedRecordToLocationTransformer ertoevt = new ExtendedRecordToLocationTransformer();
    DoFnTester<ExtendedRecord,KV<String,Location>> tester = DoFnTester.of(ertoevt);
    return tester.processBundle(getExtendedRecordArray());
  }

  public List<KV<String,IssueLineageRecord>> getSpatialIssues() throws Exception {
    ExtendedRecordToLocationTransformer ertoevt = new ExtendedRecordToLocationTransformer();
    DoFnTester<ExtendedRecord,KV<String,Location>> tester = DoFnTester.of(ertoevt);
    tester.processBundle(getExtendedRecordArray());
    return tester.peekOutputElements(ExtendedRecordToLocationTransformer.locationIssueTag);
  }

  @Test
  public void test1() throws Exception {



    p.getCoderRegistry()
      .registerCoderForClass(ExtendedRecord.class,
                            AvroCoder.of(ExtendedRecord.class));
    p.getCoderRegistry()
      .registerCoderForClass(Event.class,
                             AvroCoder.of(Event.class));
    p.getCoderRegistry()
      .registerCoderForClass(Location.class,
                             AvroCoder.of(Location.class));
    p.getCoderRegistry()
      .registerCoderForClass(ExtendedOccurence.class,
                             AvroCoder.of(ExtendedOccurence.class));
    p.getCoderRegistry()
      .registerCoderForClass(Issue.class,
                             AvroCoder.of(Issue.class));
    p.getCoderRegistry()
      .registerCoderForClass(Lineage.class,
                             AvroCoder.of(Lineage.class));
    p.getCoderRegistry()
      .registerCoderForClass(IssueLineageRecord.class,
                             AvroCoder.of(IssueLineageRecord.class));

    p.getCoderRegistry()
      .registerCoderForType(temporalCategory.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Event.class)));
    p.getCoderRegistry()
      .registerCoderForType(spatialCategory.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Location.class)));
    p.getCoderRegistry()
      .registerCoderForType(spatialCategoryIssues.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));
    p.getCoderRegistry()
      .registerCoderForType(temporalCategoryIssues.getTypeDescriptor(),
                            KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));

    PCollection<ExtendedRecord> apply = p.apply(Create.of(Arrays.asList(getExtendedRecordArray()))).setCoder(AvroCoder.of(ExtendedRecord.class));
    PCollectionTuple interpreted_records_transform =
      apply.apply("interpreted records transform", new RawToInterpretedCategoryTransformer());

    PCollection<KV<String,Event>>
      events= interpreted_records_transform.get(RawToInterpretedCategoryTransformer.temporalCategory).setCoder(
      KvCoder.of(StringUtf8Coder.of(),AvroCoder.of(Event.class)));

    PCollection<KV<String,Location>>
      spatials= interpreted_records_transform.get(RawToInterpretedCategoryTransformer.spatialCategory).setCoder(
      KvCoder.of(StringUtf8Coder.of(),AvroCoder.of(Location.class)));

    PCollection<KV<String,IssueLineageRecord>>
      eventsIssue= interpreted_records_transform.get(RawToInterpretedCategoryTransformer.temporalCategoryIssues).setCoder(
      KvCoder.of(StringUtf8Coder.of(),AvroCoder.of(IssueLineageRecord.class)));

    PCollection<KV<String,IssueLineageRecord>>
      spatialsIssue= interpreted_records_transform.get(RawToInterpretedCategoryTransformer.spatialCategoryIssues).setCoder(
      KvCoder.of(StringUtf8Coder.of(),AvroCoder.of(IssueLineageRecord.class)));

    PAssert.that(events).containsInAnyOrder(getTemporal());
    PAssert.that(eventsIssue).containsInAnyOrder(getTemporalIssues());
    PAssert.that(spatials).containsInAnyOrder(getSpatial());
    PAssert.that(spatialsIssue).containsInAnyOrder(getSpatialIssues());

    p.run();
  }




}
