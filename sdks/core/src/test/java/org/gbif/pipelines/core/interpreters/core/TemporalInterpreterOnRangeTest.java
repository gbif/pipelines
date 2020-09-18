package org.gbif.pipelines.core.interpreters.core;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.junit.Test;

public class TemporalInterpreterOnRangeTest {
  @Test
  public void testYearRange() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004/2005");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    TemporalInterpreter.interpretTemporal(er, tr);
    assertEquals(LocalDateTime.of(2004, 1, 1, 0, 0).toString(), tr.getEventDate().getGte());
    assertEquals(LocalDateTime.of(2005, 12, 31, 23, 59, 59).toString(), tr.getEventDate().getLte());
  }

  @Test
  public void testYearMonthRange() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-11/2005-02");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    TemporalInterpreter.interpretTemporal(er, tr);
    assertEquals(2004, tr.getYear().intValue());
    assertEquals(11, tr.getMonth().intValue());
    assertEquals(LocalDateTime.of(2004, 11, 1, 0, 0).toString(), tr.getEventDate().getGte());
    assertEquals(LocalDateTime.of(2005, 2, 28, 23, 59, 59).toString(), tr.getEventDate().getLte());
  }

  @Test
  public void testISOYMRange() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-02/12");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    TemporalInterpreter.interpretTemporal(er, tr);
    assertEquals(2004, tr.getYear().intValue());
    assertEquals(2, tr.getMonth().intValue());
    assertEquals(LocalDateTime.of(2004, 2, 1, 0, 0).toString(), tr.getEventDate().getGte());
    assertEquals(LocalDateTime.of(2004, 12, 31, 23, 59, 59).toString(), tr.getEventDate().getLte());
  }

  @Test
  public void test_ISO_YMRange() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-2/3");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    TemporalInterpreter.interpretTemporal(er, tr);
    assertEquals(2004, tr.getYear().intValue());
    assertEquals(2, tr.getMonth().intValue());
    assertEquals(LocalDateTime.of(2004, 2, 1, 0, 0).toString(), tr.getEventDate().getGte());
    assertEquals(LocalDateTime.of(2004, 3, 31, 23, 59, 59).toString(), tr.getEventDate().getLte());
  }

  @Test
  public void test_NONE_ISO_YMRange() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2004-2-1 to 3-2");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();
    TemporalInterpreter.interpretTemporal(er, tr);
    assertEquals(LocalDateTime.of(2004, 2, 1, 0, 0).toString(), tr.getEventDate().getGte());
    assertEquals(LocalDateTime.of(2004, 3, 2, 23, 59, 59).toString(), tr.getEventDate().getLte());

    map.put(DwcTerm.eventDate.qualifiedName(), "2004-2-1 & 3-2");
    TemporalInterpreter.interpretTemporal(er, tr);
    assertEquals(LocalDateTime.of(2004, 2, 1, 0, 0).toString(), tr.getEventDate().getGte());
    assertEquals(LocalDateTime.of(2004, 3, 2, 23, 59, 59).toString(), tr.getEventDate().getLte());
  }
}
