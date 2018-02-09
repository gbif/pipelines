package org.gbif.pipelines.core.functions.transforms;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.ExtendedRecordToEventTransformer;

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

  static ExtendedRecord VALID_INPUT() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwcTerm.day.qualifiedName(), VALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), VALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), VALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_DAY_INVALID_INPUT1() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwcTerm.day.qualifiedName(), RANGE_INVALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), VALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), VALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_INPUT2_ALL() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
    coreTerms.put(DwcTerm.day.qualifiedName(), INVALID_DAY);
    coreTerms.put(DwcTerm.month.qualifiedName(), INVALID_MONTH);
    coreTerms.put(DwcTerm.year.qualifiedName(), INVALID_YEAR);
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), BasisOfRecord.HUMAN_OBSERVATION.name());
    return ExtendedRecord.newBuilder()
      .setId(VALID_OCC_ID)
      .setCoreTerms(coreTerms)
      .setExtensions(new HashMap<>())
      .build();
  }

  static ExtendedRecord INVALID_MONTH_INVALID_INPUT2() {
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
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
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
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
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
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
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
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
    Map<CharSequence, CharSequence> coreTerms = new HashMap<CharSequence, CharSequence>();
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

  /**
   * all valid (day/month/year)
   */
  @Test
  public void valid_test1() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(VALID_INPUT());
    List<KV<String, Event>> result = tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_DATA_TAG);
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(ExtendedRecordToEventTransformer.EVENT_ISSUE_TAG);
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()));
  }

  /**
   * invalid day as null
   */
  @Test
  public void valid_test2() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(INVALID_DAY_INVALID_INPUT1());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(null, event.getDay());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()));
    //1issue and lineage on occurenceId as it is null
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.occurrenceID.name()));
    Assert.assertNull(issueLineageRecord.getFieldLineageMap().get(DwcTerm.occurrenceID.name()));

  }

  /**
   * invalid month as invalid
   */
  @Test
  public void valid_test4() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(INVALID_MONTH_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()));
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()));
    //1issue and lineage on occurenceId as it is null
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.occurrenceID.name()));
    Assert.assertNull(issueLineageRecord.getFieldLineageMap().get(DwcTerm.occurrenceID.name()));

  }

  /**
   * all invalid days/months and year
   */
  @Test
  public void valid_test3() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(INVALID_INPUT2_ALL());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(null, event.getDay());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()).size());

  }

  /**
   * invalid months
   */
  @Test
  public void valid_test5() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(INVALID_MONTH_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(Integer.parseInt(VALID_YEAR), event.getYear().intValue());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()));
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()));

  }

  /**
   * invalid year
   */
  @Test
  public void valid_test6() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(INVALID_YEAR_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()));
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()));

  }

  /**
   * invalid day and year
   */
  @Test
  public void valid_test7() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(INVALID_DAY_YEAR_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(null, event.getDay());
    Assert.assertEquals(Integer.parseInt(VALID_MONTH), event.getMonth().intValue());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()));

  }

  /**
   * invalid month and year
   */
  @Test
  public void valid_test8() throws Exception {
    ExtendedRecordToEventTransformer eventTransformer = new ExtendedRecordToEventTransformer();
    DoFnTester<ExtendedRecord, KV<String, Event>> tester = DoFnTester.of(eventTransformer);

    tester.processBundle(INVALID_MONTH_YEAR_INVALID_INPUT2());
    List<KV<String, Event>> result = tester.peekOutputElements(eventTransformer.getEventDataTag());
    String occID = result.get(0).getKey();
    Event event = result.get(0).getValue();

    Assert.assertEquals(VALID_OCC_ID, occID);
    Assert.assertEquals(Integer.parseInt(VALID_DAY), event.getDay().intValue());
    Assert.assertEquals(null, event.getMonth());
    Assert.assertEquals(null, event.getYear());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), event.getBasisOfRecord());
    List<KV<String, IssueLineageRecord>> issueResult =
      tester.peekOutputElements(eventTransformer.getEventIssueTag());
    String issue_occID = issueResult.get(0).getKey();
    IssueLineageRecord issueLineageRecord = issueResult.get(0).getValue();
    Assert.assertEquals(VALID_OCC_ID, issue_occID);
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.month.name()).size());
    Assert.assertEquals(1, issueLineageRecord.getFieldIssuesMap().get(DwcTerm.year.name()).size());
    Assert.assertNull(issueLineageRecord.getFieldIssuesMap().get(DwcTerm.day.name()));

  }
}
