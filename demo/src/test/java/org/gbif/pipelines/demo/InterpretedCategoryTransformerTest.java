package org.gbif.pipelines.demo;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.DayInterpreter;
import org.gbif.pipelines.core.functions.interpretation.InterpretationException;
import org.gbif.pipelines.core.functions.interpretation.MonthInterpreter;
import org.gbif.pipelines.core.functions.interpretation.YearInterpreter;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.transforms.DwCATermIdentifier;
import org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;

import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.spatialCategory;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.spatialCategoryIssues;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.temporalCategory;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.temporalCategoryIssues;

/**
 * Testing the RawRecordtoInterpretedCategoryPTransform.
 * This test can be further extended when the different categories are defined.
 */
public class InterpretedCategoryTransformerTest {

  static final String VALID_OCC_ID = "VALID_ID";
  static final String EMPTY_OCC_ID = "";
  static final String NULL_OCC_ID = null;
  static final String valid_day = "12";
  static final String valid_month = "2";
  static final String valid_year = "2015";
  static final String range_invalid_day = "35";
  static final String range_invalid_month = "-1";
  static final String range_invalid_year = "1";
  static final String null_day = null;
  static final String null_month = null;
  static final String null_year = null;
  static final String empty_day = "";
  static final String empty_month = "";
  static final String empty_year = "";
  static final String invalid_day = "Ja";
  static final String invalid_month = "May";
  static final String invalid_year = "Y2K";
  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  static ExtendedRecord VALID_INPUT() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    coreTerms.put(DwCATermIdentifier.country.getIdentifier(), Country.DENMARK.name());
    coreTerms.put(DwCATermIdentifier.continent.getIdentifier(), Continent.EUROPE.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_DAY_INVALID_INPUT1() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), range_invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    coreTerms.put(DwCATermIdentifier.country.getIdentifier(), "XYZ");
    coreTerms.put(DwCATermIdentifier.continent.getIdentifier(), Continent.EUROPE.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_INPUT2_ALL() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    coreTerms.put(DwCATermIdentifier.country.getIdentifier(), "XYZ");
    coreTerms.put(DwCATermIdentifier.continent.getIdentifier(), "ABC");
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_MONTH_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_YEAR_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_DAY_YEAR_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_MONTH_YEAR_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), invalid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_MONTH_DAY_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), invalid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), invalid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  public static ExtendedRecord[] getExtendedRecordArray() {
    return new ExtendedRecord[] {INVALID_MONTH_INVALID_INPUT2(), VALID_INPUT(), INVALID_INPUT2_ALL(),
      INVALID_DAY_YEAR_INVALID_INPUT2()};

  }

  public static List<KV<String, Event>> getTemporal() throws Exception {
    List<KV<String, Event>> eventsTemporal = new ArrayList<>();
    for (ExtendedRecord record : getExtendedRecordArray()) {
      Event e = new Event();
      e.setOccurrenceID(record.getId());
      e.setBasisOfRecord(record.getCoreTerms().get(DwCATermIdentifier.basisOfRecord.getIdentifier()));
      try {
        e.setDay(Integer.parseInt(record.getCoreTerms().get(DwCATermIdentifier.day.getIdentifier()).toString()));
      } catch (Exception ex) {

      }
      try {
        e.setMonth(Integer.parseInt(record.getCoreTerms().get(DwCATermIdentifier.month.getIdentifier()).toString()));
      } catch (Exception ex) {

      }
      try {
        e.setYear(Integer.parseInt(record.getCoreTerms().get(DwCATermIdentifier.year.getIdentifier()).toString()));
      } catch (Exception ex) {

      }

      eventsTemporal.add(KV.of(record.getId().toString(), e));
    }
    return eventsTemporal;
  }

  public static List<KV<String, IssueLineageRecord>> getTemporalIssues() throws Exception {
    List<KV<String, IssueLineageRecord>> temporalIssues = new ArrayList<>();
    for (ExtendedRecord record : getExtendedRecordArray()) {
      IssueLineageRecord e = new IssueLineageRecord();
      Map<CharSequence, List<Issue>> fieldIssueMap = new HashMap<>();
      Map<CharSequence, List<Lineage>> fieldLineageMap = new HashMap<>();

      try {
        new DayInterpreter().interpret(record.getCoreTerms().get(DwCATermIdentifier.day.getIdentifier()).toString());
      } catch (InterpretationException ex) {
        fieldIssueMap.put(DwCATermIdentifier.day.name(), ex.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.day.name(), ex.getLineages());
      }
      try {
        new MonthInterpreter().interpret(record.getCoreTerms()
                                           .get(DwCATermIdentifier.month.getIdentifier())
                                           .toString());
      } catch (InterpretationException ex) {
        fieldIssueMap.put(DwCATermIdentifier.month.name(), ex.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.month.name(), ex.getLineages());
      }
      try {
        new YearInterpreter().interpret(record.getCoreTerms().get(DwCATermIdentifier.year.getIdentifier()).toString());
      } catch (InterpretationException ex) {
        fieldIssueMap.put(DwCATermIdentifier.year.name(), ex.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.year.name(), ex.getLineages());
      }
      e.setOccurenceId(record.getId());
      e.setFieldIssuesMap(fieldIssueMap);
      e.setFieldLineageMap(fieldLineageMap);
      temporalIssues.add(KV.of(record.getId().toString(), e));
    }
    return temporalIssues;
  }

  /**
   * Tests data from a curated dataset with valid and invalid extendedRecords
   */
  @Test
  public void test1() throws Exception {


    /*
    setting coder registry with appropriate coders
     */
    p.getCoderRegistry().registerCoderForClass(ExtendedRecord.class, AvroCoder.of(ExtendedRecord.class));
    p.getCoderRegistry().registerCoderForClass(Event.class, AvroCoder.of(Event.class));
    p.getCoderRegistry().registerCoderForClass(Location.class, AvroCoder.of(Location.class));
    p.getCoderRegistry().registerCoderForClass(ExtendedOccurence.class, AvroCoder.of(ExtendedOccurence.class));
    p.getCoderRegistry().registerCoderForClass(Issue.class, AvroCoder.of(Issue.class));
    p.getCoderRegistry().registerCoderForClass(Lineage.class, AvroCoder.of(Lineage.class));
    p.getCoderRegistry().registerCoderForClass(IssueLineageRecord.class, AvroCoder.of(IssueLineageRecord.class));

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
    /*
    apply transformer to the pipeline and extract information from the output tuple
     */
    PCollection<ExtendedRecord> apply =
      p.apply(Create.of(Arrays.asList(getExtendedRecordArray()))).setCoder(AvroCoder.of(ExtendedRecord.class));
    PCollectionTuple interpreted_records_transform =
      apply.apply("interpreted records transform", new RawToInterpretedCategoryTransformer());

    PCollection<KV<String, Event>> events =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.temporalCategory)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Event.class)));

    PCollection<KV<String, Location>> spatials =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.spatialCategory)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Location.class)));

    PCollection<KV<String, IssueLineageRecord>> eventsIssue =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.temporalCategoryIssues)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));

    PCollection<KV<String, IssueLineageRecord>> spatialsIssue =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.spatialCategoryIssues)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));

    //verify temporal vents with expected results
    PAssert.that(events).containsInAnyOrder(getTemporal());
    PAssert.that(eventsIssue).containsInAnyOrder(getTemporalIssues());

    /*
    TodO Testing spatial transformation data and issues
     */

    p.run();

  }

}
