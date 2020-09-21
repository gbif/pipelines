package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY;
import static org.gbif.common.parsers.date.DateComponentOrdering.MDY;
import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.junit.Test;

public class TemporalInterpreterTest {
  TemporalInterpreter temporalInterpreter = TemporalInterpreter.create();

  @Test
  public void testYearMonth() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1879-10");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    temporalInterpreter.interpretTemporal(er, tr);

    assertDate("1879-10", tr.getEventDate().getGte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(10, tr.getMonth().intValue());
    assertNull(tr.getDay());

    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testYear() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1879");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    temporalInterpreter.interpretTemporal(er, tr);

    assertDate("1879", tr.getEventDate().getGte());
    assertEquals(1879, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());

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

    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);

    assertDate("2014", tr.getModified());
    assertDate("2012", tr.getDateIdentified());
    assertDate("1879", tr.getEventDate().getGte());
    assertEquals(1879, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());

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

    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);

    assertDate("2014-01-11", tr.getModified());
    assertDate("2012-01-11", tr.getDateIdentified());
    assertDate("1879-11-01", tr.getEventDate().getGte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(1, tr.getDay().intValue());

    assertEquals(0, tr.getIssues().getIssueList().size());
  }

  @Test
  public void testEventDateTimeDates() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1999-11-11T12:22");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "2012-01-11");
    map.put(DcTerm.modified.qualifiedName(), "2014-01-11");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);

    assertDate("2014-01-11", tr.getModified());
    assertDate("2012-01-11", tr.getDateIdentified());
    assertDate("1999-11-11", tr.getEventDate().getGte());
    assertEquals(1999, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(11, tr.getDay().intValue());

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

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1987-01-31");
    temporalInterpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1787-03-27");
    temporalInterpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "2014-01-11");
    temporalInterpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1997");
    temporalInterpreter.interpretDateIdentified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    er.getCoreTerms()
        .put(DwcTerm.dateIdentified.qualifiedName(), (cal.get(Calendar.YEAR) + 1) + "-01-11");
    temporalInterpreter.interpretDateIdentified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(
        OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY.name(),
        tr.getIssues().getIssueList().iterator().next());

    er.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1599-01-11");
    temporalInterpreter.interpretDateIdentified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(
        OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY.name(),
        tr.getIssues().getIssueList().iterator().next());
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

    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), "2014-01-11");
    temporalInterpreter.interpretModified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());

    tr = TemporalRecord.newBuilder().setId("1").build();
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), (cal.get(Calendar.YEAR) + 1) + "-01-11");
    temporalInterpreter.interpretModified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(
        OccurrenceIssue.MODIFIED_DATE_UNLIKELY.name(),
        tr.getIssues().getIssueList().iterator().next());

    tr = TemporalRecord.newBuilder().setId("1").build();
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), "1969-12-31");
    temporalInterpreter.interpretModified(er, tr);
    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(
        OccurrenceIssue.MODIFIED_DATE_UNLIKELY.name(),
        tr.getIssues().getIssueList().iterator().next());

    tr = TemporalRecord.newBuilder().setId("1").build();
    er.getCoreTerms().put(DcTerm.modified.qualifiedName(), "2018-10-15 16:21:48");
    temporalInterpreter.interpretModified(er, tr);
    assertEquals(0, tr.getIssues().getIssueList().size());
    assertDate("2018-10-15T16:21:48", tr.getModified());
  }

  @Test
  public void testLikelyRecorded() {
    Map<String, String> map = new HashMap<>();
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    map.put(DwcTerm.eventDate.qualifiedName(), "24.12." + (cal.get(Calendar.YEAR) + 1));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();

    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    temporalInterpreter.interpretTemporal(er, tr);

    assertEquals(1, tr.getIssues().getIssueList().size());
    assertEquals(
        OccurrenceIssue.RECORDED_DATE_UNLIKELY.name(),
        tr.getIssues().getIssueList().iterator().next());
  }

  /** Parsing ambigous date like 01/02/1999 with D/M/Y format */
  @Test
  public void testDmyDate() {
    TemporalInterpreter ti = TemporalInterpreter.create(DMY);

    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1/11/1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "02/20/1920");
    map.put(DcTerm.modified.qualifiedName(), "23/2/1940");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    ti.interpretTemporal(er, tr);
    ti.interpretModified(er, tr);
    ti.interpretDateIdentified(er, tr);

    assertDate("1940-02-23", tr.getModified());
    assertDate("1920-02-20", tr.getDateIdentified());
    assertDate("1879-11-01", tr.getEventDate().getGte());
    assertEquals(1879, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(1, tr.getDay().intValue());
  }

  @Test
  public void testMdyDate() {
    TemporalInterpreter ti = TemporalInterpreter.create(MDY);

    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "1/11/1879");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "02/20/1920");
    map.put(DcTerm.modified.qualifiedName(), "23/2/1940");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    ti.interpretTemporal(er, tr);
    ti.interpretModified(er, tr);
    ti.interpretDateIdentified(er, tr);

    assertDate("1940-02-23", tr.getModified());
    assertDate("1920-02-20", tr.getDateIdentified());
    assertDate("1879-01-11", tr.getEventDate().getGte());
  }

  /** @param expected expected date in ISO yyyy-MM-dd format */
  private void assertDate(String expected, String result) {
    if (expected == null) {
      assertNull(result);
    } else {
      assertNotNull("Missing date", result);
      assertEquals(expected, result);
    }
  }
}
