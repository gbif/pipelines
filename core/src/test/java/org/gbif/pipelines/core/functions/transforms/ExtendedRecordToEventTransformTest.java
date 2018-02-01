package org.gbif.pipelines.core.functions.transforms;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

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
public class ExtendedRecordToEventTransformTest {

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

  static ExtendedRecord VALID_INPUT() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwCATermIdentifier.day.getIdentifier(), valid_day);
    coreTerms.put(DwCATermIdentifier.month.getIdentifier(), valid_month);
    coreTerms.put(DwCATermIdentifier.year.getIdentifier(), valid_year);
    coreTerms.put(DwCATermIdentifier.basisOfRecord.getIdentifier(), BasisOfRecord.HUMAN_OBSERVATION.name());
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

  /**
   * all valid (day/month/year)
   */
  @Test
  public void valid_test1() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(VALID_INPUT());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(valid_day), event.getDay().intValue());
    Assert.assertEquals(Integer.parseInt(valid_month), event.getMonth().intValue());
    Assert.assertEquals(Integer.parseInt(valid_year), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()));
  }

  /**
   * invalid day as null
   */
  @Test
  public void valid_test2() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(INVALID_DAY_INVALID_INPUT1());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(null, event.getDay());
    Assert.assertEquals(Integer.parseInt(valid_month), event.getMonth().intValue());
    Assert.assertEquals(Integer.parseInt(valid_year), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()));
    //1issue and lineage on occurenceId as it is null
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.occurrenceID.name()));
    Assert.assertNull(issueLineageRecord.getFieldLineageMap().get(DwCATermIdentifier.occurrenceID.name()));

  }

  /**
   * invalid month as invalid
   */
  @Test
  public void valid_test4() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(INVALID_MONTH_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(valid_day), event.getDay().intValue());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(Integer.parseInt(valid_year), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()));
    //1issue and lineage on occurenceId as it is null
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.occurrenceID.name()));
    Assert.assertNull(issueLineageRecord.getFieldLineageMap().get(DwCATermIdentifier.occurrenceID.name()));

  }

  /**
   * all invalid days/months and year
   */
  @Test
  public void valid_test3() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(INVALID_INPUT2_ALL());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(null, event.getDay());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()).size());

  }

  /**
   * invalid months
   */
  @Test
  public void valid_test5() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(INVALID_MONTH_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(valid_day), event.getDay().intValue());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(Integer.parseInt(valid_year), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()));
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()));

  }

  /**
   * invalid year
   */
  @Test
  public void valid_test6() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(INVALID_YEAR_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(valid_day), event.getDay().intValue());
    Assert.assertEquals(Integer.parseInt(valid_month), event.getMonth().intValue());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()));
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()));

  }

  /**
   * invalid day and year
   */
  @Test
  public void valid_test7() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(INVALID_DAY_YEAR_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(null, event.getDay());
    Assert.assertEquals(Integer.parseInt(valid_month), event.getMonth().intValue());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()));

  }

  /**
   * invalid month and year
   */
  @Test
  public void valid_test8() throws Exception {
    ExtendedRecordToEventTransformer ertoevt = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(ertoevt);

    tester.processBundle(INVALID_MONTH_YEAR_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(valid_day), event.getDay().intValue());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.month.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwCATermIdentifier.day.name()));

  }
}
