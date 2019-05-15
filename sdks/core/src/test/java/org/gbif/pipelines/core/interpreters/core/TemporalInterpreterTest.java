package org.gbif.pipelines.core.interpreters.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.junit.Test;

import static org.gbif.api.vocabulary.OccurrenceIssue.IDENTIFIED_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY;
import static org.gbif.api.vocabulary.OccurrenceIssue.MODIFIED_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.MODIFIED_DATE_UNLIKELY;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_MISMATCH;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_UNLIKELY;
import static org.junit.Assert.assertEquals;

public class TemporalInterpreterTest {

  @Test
  public void datesWithoutIssuesTest() {

    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "10/10/2000");
    map.put(DwcTerm.year.qualifiedName(), "2000");
    map.put(DwcTerm.month.qualifiedName(), "10");
    map.put(DwcTerm.day.qualifiedName(), "10");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "1/1/2011");
    map.put(DcTerm.modified.qualifiedName(), "2/2/2012");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setCoreTerms(map).build();

    TemporalRecord tr = TemporalRecord.newBuilder().setId("777").build();

    // Expect
    TemporalRecord expected = TemporalRecord.newBuilder()
        .setId("777")
        .setEventDate(EventDate.newBuilder().setGte("2000-10-10").build())
        .setYear(2000)
        .setMonth(10)
        .setDay(10)
        .setModified("2012-02-02")
        .setDateIdentified("2011-01-01")
        .setStartDayOfYear(1)
        .setEndDayOfYear(366)
        .build();

    // When
    TemporalInterpreter.interpretEventDate(er, tr);
    TemporalInterpreter.interpretDateIdentified(er, tr);
    TemporalInterpreter.interpretModifiedDate(er, tr);
    TemporalInterpreter.interpretDayOfYear(tr);

    // Should
    assertEquals(expected, tr);

  }

  @Test
  public void datesWithInvalidIssuesTest() {

    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "10/00/2000");
    map.put(DwcTerm.year.qualifiedName(), "2000");
    map.put(DwcTerm.month.qualifiedName(), "00");
    map.put(DwcTerm.day.qualifiedName(), "10");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "31/2/2011");
    map.put(DcTerm.modified.qualifiedName(), "31/2/2012");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setCoreTerms(map).build();

    TemporalRecord tr = TemporalRecord.newBuilder().setId("777").build();

    // Expect
    TemporalRecord expected = TemporalRecord.newBuilder()
        .setId("777")
        .setIssues(
            IssueRecord.newBuilder()
                .setIssueList(Arrays.asList(RECORDED_DATE_INVALID.name(), IDENTIFIED_DATE_INVALID.name(),
                    MODIFIED_DATE_INVALID.name()))
                .build()
        )
        .build();

    // When
    TemporalInterpreter.interpretEventDate(er, tr);
    TemporalInterpreter.interpretDateIdentified(er, tr);
    TemporalInterpreter.interpretModifiedDate(er, tr);
    TemporalInterpreter.interpretDayOfYear(tr);

    // Should
    assertEquals(expected, tr);

  }

  @Test
  public void datesWithInvalidUnlikelyTest() {

    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "10/10/1599");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "31/2/1599");
    map.put(DcTerm.modified.qualifiedName(), "31/2/1599");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setCoreTerms(map).build();

    TemporalRecord tr = TemporalRecord.newBuilder().setId("777").build();

    // Expect
    TemporalRecord expected = TemporalRecord.newBuilder()
        .setId("777")
        .setEventDate(EventDate.newBuilder().build())
        .setStartDayOfYear(null)
        .setEndDayOfYear(null)
        .setIssues(
            IssueRecord.newBuilder()
                .setIssueList(Arrays.asList(RECORDED_DATE_UNLIKELY.name(), IDENTIFIED_DATE_UNLIKELY.name(),
                    MODIFIED_DATE_UNLIKELY.name()))
                .build()
        )
        .build();

    // When
    TemporalInterpreter.interpretEventDate(er, tr);
    TemporalInterpreter.interpretDateIdentified(er, tr);
    TemporalInterpreter.interpretModifiedDate(er, tr);
    TemporalInterpreter.interpretDayOfYear(tr);

    // Should
    assertEquals(expected, tr);

  }


  @Test
  public void datesWithMixedIssuesTest() {

    // State
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "10/11/2011");
    map.put(DwcTerm.year.qualifiedName(), "2000");
    map.put(DwcTerm.month.qualifiedName(), "10");
    map.put(DwcTerm.day.qualifiedName(), "10");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "1/1/1599");
    map.put(DcTerm.modified.qualifiedName(), "31/2/2012");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setCoreTerms(map).build();

    TemporalRecord tr = TemporalRecord.newBuilder().setId("777").build();

    // Expect
    TemporalRecord expected = TemporalRecord.newBuilder()
        .setId("777")
        .setEventDate(EventDate.newBuilder().setGte("2011-10-10").build())
        .setYear(2000)
        .setMonth(10)
        .setDay(10)
        .setStartDayOfYear(1)
        .setEndDayOfYear(366)
        .setIssues(
            IssueRecord.newBuilder()
                .setIssueList(Arrays.asList(RECORDED_DATE_MISMATCH.name(), IDENTIFIED_DATE_UNLIKELY.name(),
                    MODIFIED_DATE_INVALID.name()))
                .build()
        )
        .build();

    // When
    TemporalInterpreter.interpretEventDate(er, tr);
    TemporalInterpreter.interpretDateIdentified(er, tr);
    TemporalInterpreter.interpretModifiedDate(er, tr);
    TemporalInterpreter.interpretDayOfYear(tr);

    // Should
    assertEquals(expected, tr);

  }

}
