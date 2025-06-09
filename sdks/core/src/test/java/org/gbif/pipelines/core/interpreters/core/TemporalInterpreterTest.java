package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY;
import static org.gbif.api.vocabulary.OccurrenceIssue.MODIFIED_DATE_UNLIKELY;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_MISMATCH;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_UNLIKELY;
import static org.gbif.common.parsers.date.DateComponentOrdering.DMY;
import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.MDY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.time.Year;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.junit.Test;

public class TemporalInterpreterTest {

  private static final String NEXT_YEAR = Year.now().plusYears(1).toString();

  @Test
  public void testYearTerm() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("1879", tr.getEventDate().getInterval());
    assertEquals("1879-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testYearMonth() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1879-10");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("1879-10", tr.getEventDate().getInterval());
    assertEquals("1879-10-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-10-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(10, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testUsDateFormat() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2/4/2002");
    map.put(DwcTerm.year.qualifiedName(), "2002");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // Test with a not-strict parser
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("2002", tr.getEventDate().getInterval());
    assertEquals("2002-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2002-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(2002, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));

    // Test again with a strict ISO parser
    interpreter =
        TemporalInterpreter.builder()
            .orderings(Arrays.asList(DateComponentOrdering.ISO_FORMATS))
            .create();
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);

    assertEquals("2002", tr.getEventDate().getInterval());
    assertEquals("2002-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2002-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(2002, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testYear() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1879");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("1879", tr.getEventDate().getInterval());
    assertEquals("1879-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testAllDatesYear() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");
    map.put(DwcTerm.eventDate.qualifiedName(), "1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "2012");
    map.put(DcTerm.modified.qualifiedName(), "2014");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);
    interpreter.interpretModified(er, tr);
    interpreter.interpretDateIdentified(er, tr);

    assertEquals("2014", tr.getModified());
    assertEquals("2012", tr.getDateIdentified());
    assertEquals("1879", tr.getEventDate().getInterval());
    assertEquals("1879-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testAllDates() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");
    map.put(DwcTerm.month.qualifiedName(), "11 "); // keep the space at the end
    map.put(DwcTerm.day.qualifiedName(), "1");
    map.put(DwcTerm.eventDate.qualifiedName(), "1.11.1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "2012-01-11");
    map.put(DcTerm.modified.qualifiedName(), "2014-01-11");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);
    interpreter.interpretModified(er, tr);
    interpreter.interpretDateIdentified(er, tr);

    assertEquals("2014-01-11", tr.getModified());
    assertEquals("2012-01-11", tr.getDateIdentified());
    assertEquals("1879-11-01", tr.getEventDate().getInterval());
    assertEquals("1879-11-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-11-01T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(1, tr.getDay().intValue());
    assertEquals(305, tr.getStartDayOfYear().intValue());
    assertEquals(305, tr.getEndDayOfYear().intValue());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testYearDaysOnly() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");
    map.put(DwcTerm.startDayOfYear.qualifiedName(), "60 ");
    map.put(DwcTerm.endDayOfYear.qualifiedName(), " 90");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("1879-03-01/1879-03-31", tr.getEventDate().getInterval());
    assertEquals("1879-03-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-03-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(3, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertEquals(60, tr.getStartDayOfYear().intValue());
    assertEquals(90, tr.getEndDayOfYear().intValue());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testMismatchedDates() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");
    map.put(DwcTerm.month.qualifiedName(), "2");
    map.put(DwcTerm.day.qualifiedName(), "4");
    map.put(DwcTerm.eventDate.qualifiedName(), "20.1.1879");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("1879", tr.getEventDate().getInterval());
    assertEquals("1879-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_MISMATCH.name(), tr.getIssues().getIssueList().get(0));

    er.getCoreTerms().put(DwcTerm.month.qualifiedName(), "1");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertEquals("1879-01", tr.getEventDate().getInterval());
    assertEquals("1879-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-01-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(1, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_MISMATCH.name(), tr.getIssues().getIssueList().get(0));

    // The eventDate and the startDayOfYear say the day is 20 January, but day
    // still says it's the 4th, so this still isn't valid.
    er.getCoreTerms().put(DwcTerm.startDayOfYear.qualifiedName(), "20");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertEquals("1879-01", tr.getEventDate().getInterval());
    assertEquals("1879-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-01-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(1, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_MISMATCH.name(), tr.getIssues().getIssueList().get(0));

    // But the start/endDayOfYear fields are regularly a mess, so allow a consistent eventDate + ymd
    // to overrule
    er.getCoreTerms().put(DwcTerm.day.qualifiedName(), "20");
    er.getCoreTerms().put(DwcTerm.startDayOfYear.qualifiedName(), "4");
    er.getCoreTerms().put(DwcTerm.endDayOfYear.qualifiedName(), "4");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertEquals("1879-01-20", tr.getEventDate().getInterval());
    assertEquals("1879-01-20T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-01-20T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(1, tr.getMonth().intValue());
    assertEquals(20, tr.getDay().intValue());
    assertEquals(20, tr.getStartDayOfYear().intValue());
    assertEquals(20, tr.getEndDayOfYear().intValue());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testValidAndInvalidDates() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "2022");
    map.put(DwcTerm.month.qualifiedName(), "2");
    map.put(DwcTerm.day.qualifiedName(), "4");
    map.put(DwcTerm.eventDate.qualifiedName(), "20222-02-04T11:56:00");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("2022-02-04", tr.getEventDate().getInterval());
    assertEquals("2022-02-04T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2022-02-04T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(2022, tr.getYear().intValue());
    assertEquals(2, tr.getMonth().intValue());
    assertEquals(4, tr.getDay().intValue());
    assertEquals(35, tr.getStartDayOfYear().intValue());
    assertEquals(35, tr.getEndDayOfYear().intValue());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));

    // Other way around
    er.getCoreTerms().put(DwcTerm.year.qualifiedName(), "20222");
    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "2022-02-04T11:56:00");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);

    assertEquals("2022-02-04T11:56", tr.getEventDate().getInterval());
    assertEquals("2022-02-04T11:56:00.000", tr.getEventDate().getGte());
    assertEquals("2022-02-04T11:56:00.000", tr.getEventDate().getLte());
    assertEquals(2022, tr.getYear().intValue());
    assertEquals(2, tr.getMonth().intValue());
    assertEquals(4, tr.getDay().intValue());
    assertEquals(35, tr.getStartDayOfYear().intValue());
    assertEquals(35, tr.getEndDayOfYear().intValue());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));

    //
    er.getCoreTerms().put(DwcTerm.year.qualifiedName(), "2014");
    er.getCoreTerms().put(DwcTerm.month.qualifiedName(), "11");
    er.getCoreTerms().put(DwcTerm.day.qualifiedName(), "11");
    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "2014-11-11T01:00:00");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);

    assertEquals("2014-11-11T01:00", tr.getEventDate().getInterval());
    assertEquals("2014-11-11T01:00:00.000", tr.getEventDate().getGte());
    assertEquals("2014-11-11T01:00:00.000", tr.getEventDate().getLte());
    assertEquals(2014, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(11, tr.getDay().intValue());
    assertEquals(315, tr.getStartDayOfYear().intValue());
    assertEquals(315, tr.getEndDayOfYear().intValue());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  /** The bits of date we have are consistent, but not all the parts are present. */
  @Test
  public void testIncompleteDates() {
    Map<String, String> map = new HashMap<>();
    // map.put(DwcTerm.year.qualifiedName(), "1961"); // Missing year
    map.put(DwcTerm.month.qualifiedName(), "12");
    map.put(DwcTerm.day.qualifiedName(), "21");
    map.put(DwcTerm.eventDate.qualifiedName(), "1961/12/21");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals("1961-12-21", tr.getEventDate().getInterval());
    assertEquals("1961-12-21T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1961-12-21T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1961, tr.getYear().intValue());
    assertEquals(12, tr.getMonth().intValue());
    assertEquals(21, tr.getDay().intValue());
    assertEquals(355, tr.getStartDayOfYear().intValue());
    assertEquals(355, tr.getEndDayOfYear().intValue());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));

    er.getCoreTerms().put(DwcTerm.startDayOfYear.qualifiedName(), "10");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertEquals("1961-12-21", tr.getEventDate().getInterval());
    assertEquals("1961-12-21T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1961-12-21T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1961, tr.getYear().intValue());
    assertEquals(12, tr.getMonth().intValue());
    assertEquals(21, tr.getDay().intValue());
    assertEquals(355, tr.getStartDayOfYear().intValue());
    assertEquals(355, tr.getEndDayOfYear().intValue());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testEventDateTimeDates() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1999-11-11T12:22");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "2012-01-11");
    map.put(DcTerm.modified.qualifiedName(), "2014-01-11");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);
    interpreter.interpretModified(er, tr);
    interpreter.interpretDateIdentified(er, tr);

    assertEquals("2014-01-11", tr.getModified());
    assertEquals("2012-01-11", tr.getDateIdentified());
    assertEquals("1999-11-11T12:22", tr.getEventDate().getInterval());
    assertEquals("1999-11-11T12:22:00.000", tr.getEventDate().getGte());
    assertEquals("1999-11-11T12:22:00.000", tr.getEventDate().getLte());
    assertEquals(1999, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(11, tr.getDay().intValue());
    assertEquals(315, tr.getStartDayOfYear().intValue());
    assertEquals(315, tr.getEndDayOfYear().intValue());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testLikelyIdentified() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");
    map.put(DwcTerm.month.qualifiedName(), "11 ");
    map.put(DwcTerm.day.qualifiedName(), "1");
    map.put(DwcTerm.eventDate.qualifiedName(), "1.11.1879");
    map.put(DcTerm.modified.qualifiedName(), "2014-01-11");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1987-01-31");
    interpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1787-03-27");
    interpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "2014-01-11");
    interpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1997");
    interpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), NEXT_YEAR + "-01-11");
    interpreter.interpretDateIdentified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(IDENTIFIED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));

    tr.getIssues().getIssueList().clear();
    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1752-12-31");
    interpreter.interpretDateIdentified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(IDENTIFIED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testLikelyModified() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");
    map.put(DwcTerm.month.qualifiedName(), "11 ");
    map.put(DwcTerm.day.qualifiedName(), "1");
    map.put(DwcTerm.eventDate.qualifiedName(), "1.11.1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "1987-01-31");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();

    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), "2014-01-11");
    interpreter.interpretModified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    tr = TemporalRecord.newBuilder().setId("1").build();
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), NEXT_YEAR + "-01-11");
    interpreter.interpretModified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(MODIFIED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));

    tr = TemporalRecord.newBuilder().setId("1").build();
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), "1969-12-31");
    interpreter.interpretModified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(MODIFIED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));

    tr = TemporalRecord.newBuilder().setId("1").build();
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), "2018-10-15 16:21:48");
    interpreter.interpretModified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());
    assertEquals("2018-10-15T16:21:48", tr.getModified());
  }

  @Test
  public void testLikelyRecorded() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "24.12." + NEXT_YEAR);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();

    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));

    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "1499-06-01");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));

    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "1000-01-01");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));

    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "0500-01-01");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));

    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "-0001-01-01");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));
  }

  /** Parsing ambiguous date like 01/02/1999 with D/M/Y format */
  @Test
  public void testDmyDate() {
    TemporalInterpreter ti =
        TemporalInterpreter.builder().orderings(Collections.singletonList(DMY)).create();

    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1/11/1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "02/20/1920");
    map.put(DcTerm.modified.qualifiedName(), "23/2/1940");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    ti.interpretTemporal(er, tr);
    ti.interpretModified(er, tr);
    ti.interpretDateIdentified(er, tr);

    assertEquals("1940-02-23", tr.getModified());
    assertEquals("1920-02-20", tr.getDateIdentified());
    assertEquals("1879-11-01", tr.getEventDate().getInterval());
    assertEquals("1879-11-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-11-01T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(1, tr.getDay().intValue());
    assertEquals(305, tr.getStartDayOfYear().intValue());
    assertEquals(305, tr.getEndDayOfYear().intValue());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(MODIFIED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testMdyDate() {
    TemporalInterpreter ti =
        TemporalInterpreter.builder().orderings(Collections.singletonList(MDY)).create();

    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1/11/1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "02/20/1920");
    map.put(DcTerm.modified.qualifiedName(), "23/2/1940");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    ti.interpretTemporal(er, tr);
    ti.interpretModified(er, tr);
    ti.interpretDateIdentified(er, tr);

    assertEquals("1940-02-23", tr.getModified());
    assertEquals("1920-02-20", tr.getDateIdentified());
    assertEquals("1879-01-11", tr.getEventDate().getInterval());
    assertEquals("1879-01-11T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-01-11T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(1, tr.getMonth().intValue());
    assertEquals(11, tr.getDay().intValue());
    assertEquals(11, tr.getStartDayOfYear().intValue());
    assertEquals(11, tr.getEndDayOfYear().intValue());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(MODIFIED_DATE_UNLIKELY.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testMultidayRange() {
    Map<String, String> map = new HashMap<>(4);
    map.put(DwcTerm.year.qualifiedName(), "2005");
    map.put(DwcTerm.month.qualifiedName(), "11 ");
    map.put(DwcTerm.day.qualifiedName(), "13");
    map.put(DwcTerm.eventDate.qualifiedName(), "2005-11-12/2005-11-15");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    assertEquals(2005, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertEquals(316, tr.getStartDayOfYear().intValue());
    assertEquals(319, tr.getEndDayOfYear().intValue());
    assertEquals("2005-11-12/2005-11-15", tr.getEventDate().getInterval());
    assertEquals("2005-11-12T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2005-11-15T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "2005-10-12/2005-11-15");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertEquals(2005, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(285, tr.getStartDayOfYear().intValue());
    assertEquals(319, tr.getEndDayOfYear().intValue());
    assertEquals("2005-10-12/2005-11-15", tr.getEventDate().getInterval());
    assertEquals("2005-10-12T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2005-11-15T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "2004-10-12/2005-11-15");
    tr = TemporalRecord.newBuilder().setId("1").build();
    interpreter.interpretTemporal(er, tr);
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(286, tr.getStartDayOfYear().intValue());
    assertEquals(319, tr.getEndDayOfYear().intValue());
    assertEquals("2004-10-12/2005-11-15", tr.getEventDate().getInterval());
    assertEquals("2004-10-12T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2005-11-15T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testYearMonthRangeInverted() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2005-11/2005-02");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2005, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals("2005-02/2005-11", tr.getEventDate().getInterval());
    assertEquals("2005-02-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2005-11-30T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testYearMonthRange() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-11/2005-02");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals("2004-11/2005-02", tr.getEventDate().getInterval());
    assertEquals("2004-11-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2005-02-28T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoYmRange() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-02/12");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2004, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals("2004-02/2004-12", tr.getEventDate().getInterval());
    assertEquals("2004-02-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2004-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeWithoutT() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2011-09-13 09:29:08");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2011, tr.getYear().intValue());
    assertEquals(9, tr.getMonth().intValue());
    assertEquals(13, tr.getDay().intValue());
    assertEquals(256, tr.getStartDayOfYear().intValue());
    assertEquals(256, tr.getEndDayOfYear().intValue());
    assertEquals("2011-09-13T09:29:08", tr.getEventDate().getInterval());
    assertEquals("2011-09-13T09:29:08.000", tr.getEventDate().getGte());
    assertEquals("2011-09-13T09:29:08.000", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeSecZ() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2009-02-20T08:40:01Z");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2009, tr.getYear().intValue());
    assertEquals(2, tr.getMonth().intValue());
    assertEquals(20, tr.getDay().intValue());
    assertEquals(51, tr.getStartDayOfYear().intValue());
    assertEquals(51, tr.getEndDayOfYear().intValue());
    assertEquals("2009-02-20T08:40:01Z", tr.getEventDate().getInterval());
    assertEquals("2009-02-20T08:40:01.000", tr.getEventDate().getGte());
    assertEquals("2009-02-20T08:40:01.000", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeMillisecZero() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2002-03-10T00:00:00.0");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2002, tr.getYear().intValue());
    assertEquals(3, tr.getMonth().intValue());
    assertEquals(10, tr.getDay().intValue());
    assertEquals(69, tr.getStartDayOfYear().intValue());
    assertEquals(69, tr.getEndDayOfYear().intValue());
    assertEquals("2002-03-10T00:00", tr.getEventDate().getInterval());
    assertEquals("2002-03-10T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2002-03-10T00:00:00.000", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeZone() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2018-09-19T08:50+1000");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2018, tr.getYear().intValue());
    assertEquals(9, tr.getMonth().intValue());
    assertEquals(19, tr.getDay().intValue());
    assertEquals(262, tr.getStartDayOfYear().intValue());
    assertEquals(262, tr.getEndDayOfYear().intValue());
    assertEquals("2018-09-19T08:50", tr.getEventDate().getInterval());
    assertEquals("2018-09-19T08:50:00.000", tr.getEventDate().getGte());
    assertEquals("2018-09-19T08:50:00.000", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeZoneMillisec() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2013-11-06T19:59:14.961+1000");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2013, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(6, tr.getDay().intValue());
    assertEquals(310, tr.getStartDayOfYear().intValue());
    assertEquals(310, tr.getEndDayOfYear().intValue());
    assertEquals("2013-11-06T19:59:14.961", tr.getEventDate().getInterval());
    assertEquals("2013-11-06T19:59:14.961", tr.getEventDate().getGte());
    assertEquals("2013-11-06T19:59:14.961", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeMillisec() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2013-11-06T19:59:14.961");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2013, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(6, tr.getDay().intValue());
    assertEquals(310, tr.getStartDayOfYear().intValue());
    assertEquals(310, tr.getEndDayOfYear().intValue());
    assertEquals("2013-11-06T19:59:14.961", tr.getEventDate().getInterval());
    assertEquals("2013-11-06T19:59:14.961", tr.getEventDate().getGte());
    assertEquals("2013-11-06T19:59:14.961", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeZoneMinute() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2001-03-14T00:00:00-11:00");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2001, tr.getYear().intValue());
    assertEquals(3, tr.getMonth().intValue());
    assertEquals(14, tr.getDay().intValue());
    assertEquals(73, tr.getStartDayOfYear().intValue());
    assertEquals(73, tr.getEndDayOfYear().intValue());
    assertEquals("2001-03-14T00:00", tr.getEventDate().getInterval());
    assertEquals("2001-03-14T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2001-03-14T00:00:00.000", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTimeMinuteZone() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2001-03-14T00:00:00+11");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2001, tr.getYear().intValue());
    assertEquals(3, tr.getMonth().intValue());
    assertEquals(14, tr.getDay().intValue());
    assertEquals(73, tr.getStartDayOfYear().intValue());
    assertEquals(73, tr.getEndDayOfYear().intValue());
    assertEquals("2001-03-14T00:00", tr.getEventDate().getInterval());
    assertEquals("2001-03-14T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2001-03-14T00:00:00.000", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoTextMonth() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1978-December-01");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(1978, tr.getYear().intValue());
    assertEquals(12, tr.getMonth().intValue());
    assertEquals(1, tr.getDay().intValue());
    assertEquals(335, tr.getStartDayOfYear().intValue());
    assertEquals(335, tr.getEndDayOfYear().intValue());
    assertEquals("1978-12-01", tr.getEventDate().getInterval());
    assertEquals("1978-12-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1978-12-01T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoRangeYmd() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-11-01/2005-11-02");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(306, tr.getStartDayOfYear().intValue());
    assertEquals(306, tr.getEndDayOfYear().intValue());
    assertEquals("2004-11-01/2005-11-02", tr.getEventDate().getInterval());
    assertEquals("2004-11-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2005-11-02T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testDmy() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "05-02-1978");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter =
        TemporalInterpreter.builder().orderings(Arrays.asList(DMY_FORMATS)).create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(1978, tr.getYear().intValue());
    assertEquals(2, tr.getMonth().intValue());
    assertEquals(5, tr.getDay().intValue());
    assertEquals(36, tr.getStartDayOfYear().intValue());
    assertEquals(36, tr.getEndDayOfYear().intValue());
    assertEquals("1978-02-05", tr.getEventDate().getInterval());
    assertEquals("1978-02-05T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1978-02-05T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoRangeMonthDay() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1998-9-30/10-7");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(1998, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(273, tr.getStartDayOfYear().intValue());
    assertEquals(280, tr.getEndDayOfYear().intValue());
    assertEquals("1998-09-30/1998-10-07", tr.getEventDate().getInterval());
    assertEquals("1998-09-30T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1998-10-07T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoRangeShortDateWithoutZero() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1998-9-7/30");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(1998, tr.getYear().intValue());
    assertEquals(9, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertEquals(250, tr.getStartDayOfYear().intValue());
    assertEquals(273, tr.getEndDayOfYear().intValue());
    assertEquals("1998-09-07/1998-09-30", tr.getEventDate().getInterval());
    assertEquals("1998-09-07T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1998-09-30T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIsoRangeShortDate() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1998-09-07/30");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(1998, tr.getYear().intValue());
    assertEquals(9, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertEquals(250, tr.getStartDayOfYear().intValue());
    assertEquals(273, tr.getEndDayOfYear().intValue());
    assertEquals("1998-09-07/1998-09-30", tr.getEventDate().getInterval());
    assertEquals("1998-09-07T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1998-09-30T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testShortYear() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "05/02/78");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter =
        TemporalInterpreter.builder().orderings(Arrays.asList(DMY_FORMATS)).create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testExtraZeroFn() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2011-05-00");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    SerializableFunction<String, String> fn =
        v -> {
          if (StringUtils.isNotEmpty(v)) {
            return v.replaceAll("-00", "");
          }
          return v;
        };
    TemporalInterpreter interpreter = TemporalInterpreter.builder().preprocessDateFn(fn).create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2011, tr.getYear().intValue());
    assertEquals(5, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals("2011-05", tr.getEventDate().getInterval());
    assertEquals("2011-05-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2011-05-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testTextDate() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "Fri Aug 12 15:19:20 EST 2011");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testExtraDashFn() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1978-01-");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    SerializableFunction<String, String> fn =
        v -> {
          if (StringUtils.isNotEmpty(v)) {
            return v.charAt(v.length() - 1) == '-' ? v.substring(0, v.length() - 1) : v;
          }
          return v;
        };
    TemporalInterpreter interpreter = TemporalInterpreter.builder().preprocessDateFn(fn).create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(1978, tr.getYear().intValue());
    assertEquals(1, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals("1978-01", tr.getEventDate().getInterval());
    assertEquals("1978-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1978-01-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testDateExtraZ() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2011-10-31Z");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    SerializableFunction<String, String> fn =
        v -> {
          if (StringUtils.isNotEmpty(v)
              && v.matches("([12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))Z")) {
            return v.substring(0, v.length() - 1);
          }
          return v;
        };
    TemporalInterpreter interpreter = TemporalInterpreter.builder().preprocessDateFn(fn).create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2011, tr.getYear().intValue());
    assertEquals(10, tr.getMonth().intValue());
    assertEquals(31, tr.getDay().intValue());
    assertEquals(304, tr.getStartDayOfYear().intValue());
    assertEquals(304, tr.getEndDayOfYear().intValue());
    assertEquals("2011-10-31", tr.getEventDate().getInterval());
    assertEquals("2011-10-31T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2011-10-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testRangePartYear() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1978/91");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertNull(tr.getEventDate().getInterval());
    assertNull(tr.getEventDate().getGte());
    assertNull(tr.getEventDate().getLte());
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(RECORDED_DATE_INVALID.name(), tr.getIssues().getIssueList().get(0));
  }

  @Test
  public void testTextMonthYearFn() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "Aug-2005");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    SerializableFunction<String, String> fn =
        v -> {
          if (StringUtils.isNotEmpty(v)) {
            String t = v.replaceAll("Aug", "").replaceAll("-", "");
            return t + "-08";
          }
          return v;
        };
    TemporalInterpreter interpreter =
        TemporalInterpreter.builder()
            .orderings(Arrays.asList(DMY_FORMATS))
            .preprocessDateFn(fn)
            .create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2005, tr.getYear().intValue());
    assertEquals(8, tr.getMonth().intValue());
    assertNull(tr.getDay());
    assertNull(tr.getStartDayOfYear());
    assertNull(tr.getEndDayOfYear());
    assertEquals("2005-08", tr.getEventDate().getInterval());
    assertEquals("2005-08-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2005-08-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testNoneIsoYmRangeAnd() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "05-02-1978 & 06-03-1979");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    SerializableFunction<String, String> fn =
        v -> {
          if (StringUtils.isNotEmpty(v)) {
            return v.replaceAll(" & ", "/");
          }
          return v;
        };
    TemporalInterpreter interpreter =
        TemporalInterpreter.builder()
            .orderings(Arrays.asList(DMY_FORMATS))
            .preprocessDateFn(fn)
            .create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(36, tr.getStartDayOfYear().intValue());
    assertEquals(65, tr.getEndDayOfYear().intValue());
    assertEquals("1978-02-05/1979-03-06", tr.getEventDate().getInterval());
    assertEquals("1978-02-05T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1979-03-06T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testNoneIsoYmRangeTo() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "05-02-1978 to 06-03-1979");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    // When
    SerializableFunction<String, String> fn =
        v -> {
          if (StringUtils.isNotEmpty(v)) {
            return v.replaceAll(" to ", "/");
          }
          return v;
        };
    TemporalInterpreter interpreter =
        TemporalInterpreter.builder()
            .orderings(Arrays.asList(DMY_FORMATS))
            .preprocessDateFn(fn)
            .create();
    interpreter.interpretTemporal(er, tr);

    // Should
    assertNull(tr.getYear());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(36, tr.getStartDayOfYear().intValue());
    assertEquals(65, tr.getEndDayOfYear().intValue());
    assertEquals("1978-02-05/1979-03-06", tr.getEventDate().getInterval());
    assertEquals("1978-02-05T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1979-03-06T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testNoneIsoYmRange() {
    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-2-1 to 3-2");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    SerializableFunction<String, String> fn =
        v -> {
          if (StringUtils.isNotEmpty(v)) {
            return v.replaceAll(" to ", "/").replaceAll(" & ", "/");
          }
          return v;
        };
    TemporalInterpreter interpreter = TemporalInterpreter.builder().preprocessDateFn(fn).create();

    // When
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2004, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(32, tr.getStartDayOfYear().intValue());
    assertEquals(62, tr.getEndDayOfYear().intValue());
    assertEquals("2004-02-01/2004-03-02", tr.getEventDate().getInterval());
    assertEquals("2004-02-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2004-03-02T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());

    // State
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-2-1 & 3-2");

    // When
    interpreter.interpretTemporal(er, tr);

    // Should
    assertEquals(2004, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
    assertEquals(32, tr.getStartDayOfYear().intValue());
    assertEquals(62, tr.getEndDayOfYear().intValue());
    assertEquals("2004-02-01/2004-03-02", tr.getEventDate().getInterval());
    assertEquals("2004-02-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("2004-03-02T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testIssueDatesYear() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "1879");
    map.put(DwcTerm.eventDate.qualifiedName(), "1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "2012 oct");
    map.put(DcTerm.modified.qualifiedName(), "2014 oct");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter interpreter = TemporalInterpreter.builder().create();
    interpreter.interpretTemporal(er, tr);
    interpreter.interpretModified(er, tr);
    interpreter.interpretDateIdentified(er, tr);

    assertNull(tr.getModified());
    assertNull("2012", tr.getDateIdentified());
    assertEquals("1879-01-01T00:00:00.000", tr.getEventDate().getGte());
    assertEquals("1879-12-31T23:59:59.999", tr.getEventDate().getLte());
    assertEquals(1879, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());

    assertEquals(2, tr.getIssues().getIssueList().size());
  }
}
