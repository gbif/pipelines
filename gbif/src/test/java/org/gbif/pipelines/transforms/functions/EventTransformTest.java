package org.gbif.pipelines.transforms.functions;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.function.EventTransform;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for EventTransformer
 */
public class EventTransformTest {

  private static final String VALID_OCC_ID = "VALID_ID";

  private static final String VALID_DAY = "12";
  private static final String VALID_MONTH = "2";
  private static final String VALID_YEAR = "2015";

  private static final String RANGE_INVALID_DAY = "35";

  private static final String INVALID_DAY = "Ja";
  private static final String INVALID_MONTH = "May";
  private static final String INVALID_YEAR = "Y2K";

  private static ExtendedRecord buildTestData(String dayValue, String monthValue, String yearValue) {
    Map<String, String> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.day.qualifiedName(), dayValue);
    coreTerms.put(DwcTerm.month.qualifiedName(), monthValue);
    coreTerms.put(DwcTerm.year.qualifiedName(), yearValue);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(Collections.emptyMap())
      .build();
  }

  private static ExtendedRecord validInput() {
    return buildTestData(VALID_DAY, VALID_MONTH, VALID_YEAR);
  }

  private static ExtendedRecord invalidDayRangeInput() {
    return buildTestData(RANGE_INVALID_DAY, VALID_MONTH, VALID_YEAR);
  }

  private static ExtendedRecord invalidDayInput() {
    return buildTestData(INVALID_DAY, VALID_MONTH, VALID_YEAR);
  }

  private static ExtendedRecord invalidMonthInput() {
    return buildTestData(VALID_DAY, INVALID_MONTH, VALID_YEAR);
  }

  private static ExtendedRecord invalidYearInvalidInput() {
    return buildTestData(VALID_DAY, VALID_MONTH, INVALID_YEAR);
  }

  private static ExtendedRecord invalidDayYearInvalidInput() {
    return buildTestData(INVALID_DAY, VALID_MONTH, INVALID_YEAR);
  }

  private static ExtendedRecord invalidMonthYearInvalidInput() {
    return buildTestData(VALID_DAY, INVALID_MONTH, INVALID_YEAR);
  }

  /**
   * all valid (day/month/year)
   */
  @Test
  public void testValidDate() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(validInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccid = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccid);
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()));
  }

  /**
   * invalid day as null
   */
  @Test
  public void testInvalidDayRange() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(invalidDayRangeInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertNull(event.getDay());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccid = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccid);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()));
    //1issue and lineage on occurenceId as it is null
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.occurrenceID.name()));
    Assert.assertNull(issueLineageRecord.getFieldLineageMap().get(DwcTerm.occurrenceID.name()));

  }

  /**
   * invalid month as invalid
   */
  @Test
  public void testInvalidMonthInput() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(invalidMonthInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertNull(event.getMonth());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccid = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccid);
    System.out.print(issueLineageRecord);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()));
    //1issue and lineage on occurenceId as it is null
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.occurrenceID.name()));
    Assert.assertNull(issueLineageRecord.getFieldLineageMap().get(DwcTerm.occurrenceID.name()));

  }

  /**
   * all invalid days/months and year
   */
  @Test
  public void testInvalidDay() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(invalidDayInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertNull(event.getDay());
    Assert.assertNotNull(event.getMonth());
    Assert.assertNotNull(event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()));

  }

  /**
   * invalid months
   */
  @Test
  public void testInvalidMonth() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(invalidMonthInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertNull(event.getMonth());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccID);
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()));
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()));

  }

  /**
   * invalid year
   */
  @Test
  public void testInvalidYearInvalid() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(invalidYearInvalidInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertNull(event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccID);
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()));
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()));

  }

  /**
   * invalid day and year
   */
  @Test
  public void testInvalidDayYearInvalidInput() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(invalidDayYearInvalidInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertNull(event.getDay());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertNull(event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()));

  }

  /**
   * invalid month and year
   */
  @Test
  public void testInvalidMonthYearInvalidInput() throws Exception {
    EventTransform eventTransformer = new EventTransform();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(invalidMonthYearInvalidInput());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertNull(event.getMonth());
    Assert.assertNull(event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult = tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issueOccID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issueOccID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.month.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssueMap().get(DwcTerm.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssueMap().get(DwcTerm.day.name()));

  }
}
