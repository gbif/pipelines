package au.org.ala.pipelines.interpreters;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.util.HashMap;
import java.util.Map;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.junit.Test;

public class AlaTemporalInterpreterTest {

  public static TemporalRecord create(String date) {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), date);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter temporalInterpreter = TemporalInterpreter.builder().create();
    temporalInterpreter.interpretTemporal(er, tr);
    return tr;
  }

  @Test
  public void testDatePrecisionDayRange() {
    TemporalRecord tr = create("2000-01-01/2000-01-05");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals("2000-01-01", tr.getEventDate().getGte());
    assertEquals("2000-01-05", tr.getEventDate().getLte());
    assertEquals(ALATemporalInterpreter.DAY_RANGE_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionDayRange1() {
    TemporalRecord tr = create("2000-01-01/2000-01-01");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals("2000-01-01", tr.getEventDate().getGte());
    assertEquals(null, tr.getEventDate().getLte());
    assertEquals(ALATemporalInterpreter.DAY_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionDayRange2() {
    TemporalRecord tr = create("2000-01-01");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals("2000-01-01", tr.getEventDate().getGte());
    assertEquals(null, tr.getEventDate().getLte());
    assertEquals(ALATemporalInterpreter.DAY_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionMonthRange() {
    TemporalRecord tr = create("2000-01/2000-02");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals(tr.getEventDate().getGte(), "2000-01");
    assertEquals(tr.getEventDate().getLte(), "2000-02");
    assertEquals(ALATemporalInterpreter.MONTH_RANGE_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionMonth() {
    TemporalRecord tr = create("2000-01/2000-01");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals(tr.getEventDate().getGte(), "2000-01");
    assertEquals(tr.getEventDate().getLte(), null);
    assertEquals(ALATemporalInterpreter.MONTH_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionMonth2() {
    TemporalRecord tr = create("2000-01");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals(tr.getEventDate().getGte(), "2000-01");
    assertEquals(tr.getEventDate().getLte(), null);
    assertEquals(ALATemporalInterpreter.MONTH_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionYear() {
    TemporalRecord tr = create("2000/2001");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals(tr.getEventDate().getGte(), "2000");
    assertEquals(tr.getEventDate().getLte(), "2001");
    assertEquals(ALATemporalInterpreter.YEAR_RANGE_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionYear2() {
    TemporalRecord tr = create("2000/2000");
    ALATemporalInterpreter.checkDatePrecision(null, tr);
    assertEquals(tr.getEventDate().getGte(), "2000");
    assertEquals(null, tr.getEventDate().getLte());
    assertEquals(ALATemporalInterpreter.YEAR_PRECISION, tr.getDatePrecision());
  }

  @Test
  public void testDatePrecisionYearRange() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2000/2001");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter temporalInterpreter = TemporalInterpreter.builder().create();
    temporalInterpreter.interpretTemporal(er, tr);

    assertEquals(tr.getEventDate().getGte(), "2000");
    assertEquals(tr.getEventDate().getLte(), "2001");
  }

  @Test
  public void testQualityAssertion() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "");
    map.put(DwcTerm.month.qualifiedName(), " "); // keep the space at the end
    map.put(DwcTerm.day.qualifiedName(), "");
    map.put(DwcTerm.eventDate.qualifiedName(), "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter temporalInterpreter = TemporalInterpreter.builder().create();
    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    ALATemporalInterpreter.checkRecordDateQuality(er, tr);
    ALATemporalInterpreter.checkDateIdentified(er, tr);
    ALATemporalInterpreter.checkGeoreferencedDate(er, tr);

    assertArrayEquals(
        tr.getIssues().getIssueList().toArray(),
        new String[] {ALAOccurrenceIssue.MISSING_COLLECTION_DATE.name()});

    map.put(DwcTerm.year.qualifiedName(), "2000");
    map.put(DwcTerm.month.qualifiedName(), "01"); // keep the space at the end
    map.put(DwcTerm.day.qualifiedName(), "01");
    map.put(DwcTerm.eventDate.qualifiedName(), "");
    er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    tr = TemporalRecord.newBuilder().setId("1").build();

    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    ALATemporalInterpreter.checkRecordDateQuality(er, tr);
    ALATemporalInterpreter.checkDateIdentified(er, tr);
    ALATemporalInterpreter.checkGeoreferencedDate(er, tr);

    assertArrayEquals(
        new String[] {
          ALAOccurrenceIssue.FIRST_OF_MONTH.name(),
          ALAOccurrenceIssue.FIRST_OF_YEAR.name(),
          ALAOccurrenceIssue.FIRST_OF_CENTURY.name()
        },
        tr.getIssues().getIssueList().toArray());
  }

  @Test
  public void testALAssertions() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "");
    map.put(DwcTerm.month.qualifiedName(), " "); // keep the space at the end
    map.put(DwcTerm.day.qualifiedName(), "");
    map.put(DwcTerm.eventDate.qualifiedName(), "1980-2-2");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "1979-2-3");
    map.put(DwcTerm.georeferencedDate.qualifiedName(), "1981-1-1");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter temporalInterpreter = TemporalInterpreter.builder().create();
    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    ALATemporalInterpreter.checkRecordDateQuality(er, tr);
    ALATemporalInterpreter.checkDateIdentified(er, tr);
    ALATemporalInterpreter.checkGeoreferencedDate(er, tr);

    assertArrayEquals(
        new String[] {
          ALAOccurrenceIssue.ID_PRE_OCCURRENCE.name(),
          ALAOccurrenceIssue.GEOREFERENCE_POST_OCCURRENCE.name()
        },
        tr.getIssues().getIssueList().toArray());
  }

  @Test
  public void testAUformatDateAssertions() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.year.qualifiedName(), "");
    map.put(DwcTerm.month.qualifiedName(), " "); // keep the space at the end
    map.put(DwcTerm.day.qualifiedName(), "");
    map.put(DwcTerm.eventDate.qualifiedName(), "2/2/1980");
    map.put(DwcTerm.dateIdentified.qualifiedName(), "1979-2-3");
    map.put(DwcTerm.georeferencedDate.qualifiedName(), "1981-1-1");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter temporalInterpreter = TemporalInterpreter.builder().create();
    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    ALATemporalInterpreter.checkRecordDateQuality(er, tr);
    ALATemporalInterpreter.checkDateIdentified(er, tr);
    ALATemporalInterpreter.checkGeoreferencedDate(er, tr);

    assertArrayEquals(
        new String[] {
          ALAOccurrenceIssue.ID_PRE_OCCURRENCE.name(),
          ALAOccurrenceIssue.GEOREFERENCE_POST_OCCURRENCE.name()
        },
        tr.getIssues().getIssueList().toArray());
  }

  @Test
  public void testInvalidAssertions() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.qualifiedName(), "2/2/1599");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("1").build();

    TemporalInterpreter temporalInterpreter = TemporalInterpreter.builder().create();
    temporalInterpreter.interpretTemporal(er, tr);
    temporalInterpreter.interpretDateIdentified(er, tr);
    temporalInterpreter.interpretModified(er, tr);
    ALATemporalInterpreter.checkRecordDateQuality(er, tr);
    ALATemporalInterpreter.checkDateIdentified(er, tr);
    ALATemporalInterpreter.checkGeoreferencedDate(er, tr);

    assertArrayEquals(
        new String[] {OccurrenceIssue.RECORDED_DATE_UNLIKELY.name()},
        tr.getIssues().getIssueList().toArray());
  }
}
