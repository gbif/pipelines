package org.gbif.pipelines.demo;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.interpretation.DayInterpreter;
import org.gbif.pipelines.core.functions.interpretation.InterpretationException;
import org.gbif.pipelines.core.functions.interpretation.MonthInterpreter;
import org.gbif.pipelines.core.functions.interpretation.YearInterpreter;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
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
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;

import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.SPATIAL_CATEGORY;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.SPATIAL_CATEGORY_ISSUES;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.TEMPORAL_CATEGORY;
import static org.gbif.pipelines.core.functions.transforms.RawToInterpretedCategoryTransformer.TEMPORAL_CATEGORY_ISSUES;

/**
 * Testing the RawRecordtoInterpretedCategoryPTransform.
 * This test can be further extended when the different categories are defined.
 */
public class InterpretedCategoryTransformerTest {

  static final String VALID_OCC_ID = "VALID_ID";
  static final String EMPTY_OCC_ID = "";
  static final String NULL_OCC_ID = null;
  static final String VALID_DAY = "12";
  static final String VALID_MONTH = "2";
  static final String VALID_YEAR = "2015";
  static final String RANGE_INVALID_DAY = "35";
  static final String RANGE_INVALID_MONTH = "-1";
  static final String RANGE_INVALID_YEAR = "1";
  static final String NULL_DAY = null;
  static final String NULL_MONTH = null;
  static final String NULL_YEAR = null;
  static final String EMPTY_DAY = "";
  static final String EMPTY_MONTH = "";
  static final String EMPTY_YEAR = "";
  static final String INVALID_DAY = "Ja";
  static final String INVALID_MONTH = "May";
  static final String INVALID_YEAR = "Y2K";
  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  static ExtendedRecord VALID_INPUT() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), VALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), VALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), VALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    coreTerms.put(DwcTerm.country.qualifiedName(), Country.DENMARK.name());
    coreTerms.put(DwcTerm.continent.qualifiedName(), Continent.EUROPE.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_DAY_INVALID_INPUT1() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), RANGE_INVALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), VALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), VALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    coreTerms.put(DwcTerm.country.qualifiedName(), "XYZ");
    coreTerms.put(DwcTerm.continent.qualifiedName(), Continent.EUROPE.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_INPUT2_ALL() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), INVALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), INVALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), INVALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    coreTerms.put(DwcTerm.country.qualifiedName(), "XYZ");
    coreTerms.put(DwcTerm.continent.qualifiedName(), "ABC");
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_MONTH_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), VALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), INVALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), VALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_YEAR_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), VALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), VALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), INVALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_DAY_YEAR_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), INVALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), VALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), INVALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_MONTH_YEAR_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), VALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), INVALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), INVALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_MONTH_DAY_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), INVALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), INVALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), VALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  private static ExtendedRecord[] getExtendedRecordArray() {
    return new ExtendedRecord[] {INVALID_MONTH_INVALID_INPUT2(), VALID_INPUT(), INVALID_INPUT2_ALL(),
      INVALID_DAY_YEAR_INVALID_INPUT2()};

  }

  private static List<KV<String, Event>> getTemporal() {
    List<KV<String, Event>> eventsTemporal = new ArrayList<>();
    for (ExtendedRecord record : getExtendedRecordArray()) {
      Event e = new Event();
      e.setOccurrenceID(record.getId());
      e.setBasisOfRecord(record.getCoreTerms().get(DwcTerm.basisOfRecord.qualifiedName()));
      try {
        e.setDay(Integer.parseInt(record.getCoreTerms().get(DwcTerm.day.qualifiedName()).toString()));
      } catch (Exception ex) {

      }
      try {
        e.setMonth(Integer.parseInt(record.getCoreTerms().get(DwcTerm.month.qualifiedName()).toString()));
      } catch (Exception ex) {

      }
      try {
        e.setYear(Integer.parseInt(record.getCoreTerms().get(DwcTerm.year.qualifiedName()).toString()));
      } catch (Exception ex) {

      }

      eventsTemporal.add(KV.of(record.getId().toString(), e));
    }
    return eventsTemporal;
  }

  private static List<KV<String, IssueLineageRecord>> getTemporalIssues() {
    List<KV<String, IssueLineageRecord>> temporalIssues = new ArrayList<>();
    for (ExtendedRecord record : getExtendedRecordArray()) {
      IssueLineageRecord e = new IssueLineageRecord();
      Map<CharSequence, List<Issue>> fieldIssueMap = new HashMap<>();
      Map<CharSequence, List<Lineage>> fieldLineageMap = new HashMap<>();

      try {
        new DayInterpreter().interpret(record.getCoreTerms().get(DwcTerm.day.qualifiedName()).toString());
      } catch (InterpretationException ex) {
        fieldIssueMap.put(DwcTerm.day.name(), ex.getIssues());
        fieldLineageMap.put(DwcTerm.day.name(), ex.getLineages());
      }
      try {
        new MonthInterpreter().interpret(record.getCoreTerms()
                                           .get(DwcTerm.month.qualifiedName())
                                           .toString());
      } catch (InterpretationException ex) {
        fieldIssueMap.put(DwcTerm.month.name(), ex.getIssues());
        fieldLineageMap.put(DwcTerm.month.name(), ex.getLineages());
      }
      try {
        new YearInterpreter().interpret(record.getCoreTerms().get(DwcTerm.year.qualifiedName()).toString());
      } catch (InterpretationException ex) {
        fieldIssueMap.put(DwcTerm.year.name(), ex.getIssues());
        fieldLineageMap.put(DwcTerm.year.name(), ex.getLineages());
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
  public void testInterpretedCategoryTransformerMixed() throws Exception {
    /*
    setting coder registry with appropriate coders
     */
    Coders.registerAvroCoders(p,
                              ExtendedRecord.class,
                              Event.class,
                              Location.class,
                              ExtendedOccurence.class,
                              Issue.class,
                              Lineage.class,
                              IssueLineageRecord.class);
    Coders.registerAvroCodersForKVTypes(p,
                                        new TupleTag[] {TEMPORAL_CATEGORY, SPATIAL_CATEGORY, TEMPORAL_CATEGORY_ISSUES,
                                          SPATIAL_CATEGORY_ISSUES},
                                        Event.class,
                                        Location.class,
                                        IssueLineageRecord.class,
                                        IssueLineageRecord.class);
    /*
    apply transformer to the pipeline and extract information from the output tuple
     */
    PCollection<ExtendedRecord> apply =
      p.apply(Create.of(Arrays.asList(getExtendedRecordArray()))).setCoder(AvroCoder.of(ExtendedRecord.class));
    PCollectionTuple interpreted_records_transform =
      apply.apply("interpreted records transform", new RawToInterpretedCategoryTransformer());

    PCollection<KV<String, Event>> events =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.TEMPORAL_CATEGORY)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Event.class)));

    PCollection<KV<String, Location>> spatials =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.SPATIAL_CATEGORY)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Location.class)));

    PCollection<KV<String, IssueLineageRecord>> eventsIssue =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.TEMPORAL_CATEGORY_ISSUES)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));

    PCollection<KV<String, IssueLineageRecord>> spatialsIssue =
      interpreted_records_transform.get(RawToInterpretedCategoryTransformer.SPATIAL_CATEGORY_ISSUES)
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
