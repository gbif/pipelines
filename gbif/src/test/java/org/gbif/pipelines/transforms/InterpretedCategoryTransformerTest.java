package org.gbif.pipelines.transforms;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.dwca.avro.ExtendedOccurrence;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.interpretation.column.InterpretationFactory;
import org.gbif.pipelines.interpretation.column.InterpretationResult;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.InterpretedCategoryTransform;

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

/**
 * Testing the RawRecordtoInterpretedCategoryPTransform.
 * This test can be further extended when the different categories are defined.
 */
public class InterpretedCategoryTransformerTest {

  private static final String VALID_OCC_ID = "VALID_ID";
  private static final String VALID_DAY = "12";
  private static final String VALID_MONTH = "2";
  private static final String VALID_YEAR = "2015";
  private static final String RANGE_INVALID_DAY = "35";
  private static final String INVALID_DAY = "Ja";
  private static final String INVALID_MONTH = "May";
  private static final String INVALID_YEAR = "Y2K";
  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  private static ExtendedRecord VALID_INPUT() {
    Map<String, String> coreTerms = new HashMap<>();
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

  private static ExtendedRecord INVALID_INPUT2_ALL() {
    Map<String, String> coreTerms = new HashMap<>();
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

  private static ExtendedRecord INVALID_MONTH_INVALID_INPUT2() {
    Map<String, String> coreTerms = new HashMap<>();
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

  private static ExtendedRecord INVALID_DAY_YEAR_INVALID_INPUT2() {
    Map<String, String> coreTerms = new HashMap<>();
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
        e.setDay(Integer.parseInt(record.getCoreTerms().get(DwcTerm.day.qualifiedName())));
      } catch (Exception ignored) {

      }
      try {
        e.setMonth(Integer.parseInt(record.getCoreTerms().get(DwcTerm.month.qualifiedName())));
      } catch (Exception ignored) {

      }
      try {
        e.setYear(Integer.parseInt(record.getCoreTerms().get(DwcTerm.year.qualifiedName())));
      } catch (Exception ignored) {

      }

      eventsTemporal.add(KV.of(record.getId(), e));
    }
    return eventsTemporal;
  }

  private static List<KV<String, IssueLineageRecord>> getTemporalIssues() {
    List<KV<String, IssueLineageRecord>> temporalIssues = new ArrayList<>();
    for (ExtendedRecord record : getExtendedRecordArray()) {
      IssueLineageRecord e = new IssueLineageRecord();
      Map<String, List<Issue>> fieldIssueMap = new HashMap<>();
      Map<String, List<Lineage>> fieldLineageMap = new HashMap<>();

      InterpretationResult<Integer> interpretDay =
        InterpretationFactory.interpret(DwcTerm.day, record.getCoreTerms().get(DwcTerm.day.qualifiedName()));
      if (!interpretDay.isSuccessFull()) {
        fieldIssueMap.put(DwcTerm.day.name(), interpretDay.getIssueList());
        fieldLineageMap.put(DwcTerm.day.name(), interpretDay.getLineageList());
      }
      InterpretationResult<Integer> interpretMonth = InterpretationFactory.interpret(DwcTerm.month,
                                                                                     record.getCoreTerms()
                                                                                       .get(DwcTerm.month.qualifiedName()));
      if (!interpretMonth.isSuccessFull()) {
        fieldIssueMap.put(DwcTerm.month.name(), interpretMonth.getIssueList());
        fieldLineageMap.put(DwcTerm.month.name(), interpretMonth.getLineageList());
      }
      InterpretationResult<Integer> interpretYear = InterpretationFactory.interpret(DwcTerm.year,
                                                                                    record.getCoreTerms()
                                                                                      .get(DwcTerm.year.qualifiedName()));
      if (!interpretYear.isSuccessFull()) {
        fieldIssueMap.put(DwcTerm.year.name(), interpretYear.getIssueList());
        fieldLineageMap.put(DwcTerm.year.name(), interpretYear.getLineageList());
      }
      e.setOccurenceId(record.getId());
      e.setFieldIssueMap(fieldIssueMap);
      e.setFieldLineageMap(fieldLineageMap);
      temporalIssues.add(KV.of(record.getId(), e));
    }
    return temporalIssues;
  }

  /**
   * Tests data from a curated dataset with valid and invalid extendedRecords
   */
  @Test
  public void testInterpretedCategoryTransformerMixed() {
    /*
    setting coder registry with appropriate coders
     */

    Coders.registerAvroCoders(p,
                              ExtendedRecord.class,
                              Event.class,
                              Location.class,
                              ExtendedOccurrence.class,
                              Issue.class,
                              Lineage.class,
                              IssueLineageRecord.class);
    InterpretedCategoryTransform transformer = new InterpretedCategoryTransform();
    Coders.registerAvroCodersForKVTypes(p,
                                        new TupleTag[] {transformer.getTemporalCategory(),
                                          transformer.getSpatialCategory(), transformer.getTemporalCategoryIssues(),
                                          transformer.getSpatialCategoryIssues()},
                                        Event.class,
                                        Location.class,
                                        IssueLineageRecord.class,
                                        IssueLineageRecord.class);
    /*
    apply transformer to the pipeline and extract information from the output tuple
     */
    PCollection<ExtendedRecord> apply =
      p.apply(Create.of(Arrays.asList(getExtendedRecordArray()))).setCoder(AvroCoder.of(ExtendedRecord.class));
    PCollectionTuple interpreted_records_transform = apply.apply("interpreted records transform", transformer);

    PCollection<KV<String, Event>> events = interpreted_records_transform.get(transformer.getTemporalCategory())
      .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Event.class)));

    PCollection<KV<String, Location>> spatials = interpreted_records_transform.get(transformer.getSpatialCategory())
      .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Location.class)));

    PCollection<KV<String, IssueLineageRecord>> eventsIssue =
      interpreted_records_transform.get(transformer.getTemporalCategoryIssues())
        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(IssueLineageRecord.class)));

    PCollection<KV<String, IssueLineageRecord>> spatialsIssue =
      interpreted_records_transform.get(transformer.getSpatialCategoryIssues())
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
