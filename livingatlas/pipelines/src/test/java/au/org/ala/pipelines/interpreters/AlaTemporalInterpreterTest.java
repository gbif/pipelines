package au.org.ala.pipelines.interpreters;

import static org.junit.Assert.assertArrayEquals;

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
  public void testAUformatDatessertions() {
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
