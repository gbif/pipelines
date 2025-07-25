package org.gbif.pipelines.core.interpreters.extension;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.MockVocabularyLookups;
import org.gbif.pipelines.core.parsers.humboldt.DurationUnit;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.junit.Test;

public class HumboldtInterpreterTest {

  @Test
  public void humboldtTest() {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> humboldtData = new HashMap<>(2);
    humboldtData.put("http://rs.tdwg.org/eco/terms/siteCount", "-1");
    humboldtData.put("http://rs.tdwg.org/eco/terms/verbatimSiteDescriptions", "sd1 |sd2");
    humboldtData.put("http://rs.tdwg.org/eco/terms/geospatialScopeAreaValue", "10.1");
    humboldtData.put("http://rs.tdwg.org/eco/terms/geospatialScopeAreaUnit", "km²");
    humboldtData.put("http://rs.tdwg.org/eco/terms/totalAreaSampledValue", "20");
    humboldtData.put("http://rs.tdwg.org/eco/terms/totalAreaSampledUnit", "km²");
    humboldtData.put("http://rs.tdwg.org/eco/terms/samplingEffortValue", "20");
    humboldtData.put("http://rs.tdwg.org/eco/terms/eventDurationValue", "3");
    humboldtData.put("http://rs.tdwg.org/eco/terms/eventDurationUnit", "Minutes");
    humboldtData.put("http://rs.tdwg.org/eco/terms/isAbsenceReported", "True");
    humboldtData.put("http://rs.tdwg.org/eco/terms/hasNonTargetTaxa", "True in some cases");
    humboldtData.put("http://rs.tdwg.org/eco/terms/targetLifeStageScope", "ADUlt");
    humboldtData.put("http://rs.tdwg.org/eco/terms/targetDegreeOfEstablishmentScope", "td1| td2");

    ext.put("http://rs.tdwg.org/eco/terms/Event", List.of(humboldtData));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    HumboldtRecord hr = HumboldtRecord.newBuilder().setId("id").build();

    // When
    // TODO: use kv
    HumboldtInterpreter.builder()
        .vocabularyService(
            VocabularyService.builder()
                .vocabularyLookup(
                    DwcTerm.lifeStage.qualifiedName(),
                    new MockVocabularyLookups.LifeStageMockVocabularyLookup())
                .vocabularyLookup(
                    DwcTerm.degreeOfEstablishment.qualifiedName(),
                    new MockVocabularyLookups.GenericMockVocabularyLookup())
                .build())
        .create()
        .interpret(er, hr);

    // Should
    assertEquals("id", hr.getId());
    assertEquals(1, hr.getHumboldtItems().size());

    Humboldt humboldt = hr.getHumboldtItems().get(0);
    assertNull(humboldt.getSiteCount());
    assertEquals(2, humboldt.getVerbatimSiteDescriptions().size());
    assertEquals(10.1, humboldt.getGeospatialScopeAreaValue(), 0.0001);
    assertEquals("km²", humboldt.getGeospatialScopeAreaUnit());
    assertEquals(20.0, humboldt.getTotalAreaSampledValue(), 0.0001);
    assertEquals("km²", humboldt.getTotalAreaSampledUnit());
    assertEquals(20.0, humboldt.getSamplingEffortValue(), 0.0001);
    assertEquals(3.0, humboldt.getEventDurationValue(), 0.0001);
    assertEquals(DurationUnit.minutes.name(), humboldt.getEventDurationUnit());
    assertTrue(humboldt.getIsAbsenceReported());
    assertNull(humboldt.getHasNonTargetTaxa());
    assertEquals(1, humboldt.getTargetLifeStageScope().size());
    assertEquals("Adult", humboldt.getTargetLifeStageScope().get(0).getConcept());
    assertEquals(2, humboldt.getTargetDegreeOfEstablishmentScope().size());

    assertTrue(
        hr.getIssues()
            .getIssueList()
            .contains(
                OccurrenceIssue.GEOSPATIAL_SCOPE_AREA_LOWER_THAN_TOTAL_TOTAL_AREA_SAMPLED.name()));
    assertTrue(hr.getIssues().getIssueList().contains(OccurrenceIssue.SITE_COUNT_INVALID.name()));
    assertTrue(
        hr.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.SAMPLING_EFFORT_UNIT_MISSING.name()));
    assertTrue(
        hr.getIssues().getIssueList().contains(OccurrenceIssue.HAS_NON_TARGET_TAXA_INVALID.name()));
  }

  @Test
  public void multipleExtensionsPerEventTest() {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> humboldtData = new HashMap<>(2);
    humboldtData.put("http://rs.tdwg.org/eco/terms/geospatialScopeAreaValue", "10");

    Map<String, String> humboldtData2 = new HashMap<>(2);
    humboldtData2.put("http://rs.tdwg.org/eco/terms/geospatialScopeAreaValue", "20");

    ext.put("http://rs.tdwg.org/eco/terms/Event", List.of(humboldtData, humboldtData2));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    HumboldtRecord hr = HumboldtRecord.newBuilder().setId("id").build();

    // When
    HumboldtInterpreter.builder().create().interpret(er, hr);

    // Should
    assertEquals("id", hr.getId());
    assertEquals(2, hr.getHumboldtItems().size());
    assertEquals(10.0, hr.getHumboldtItems().get(0).getGeospatialScopeAreaValue(), 0.0001);
    assertEquals(20.0, hr.getHumboldtItems().get(1).getGeospatialScopeAreaValue(), 0.0001);
    assertTrue(
        hr.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.GEOSPATIAL_SCOPE_AREA_UNIT_MISSING.name()));
    assertEquals(1, hr.getIssues().getIssueList().size());
  }

  @Test
  public void emptyExtensionTest() {
    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").build();
    HumboldtRecord hr = HumboldtRecord.newBuilder().setId("id").build();

    // When
    HumboldtInterpreter.builder().create().interpret(er, hr);

    // Should
    assertEquals("[]", hr.getHumboldtItems().toString());
    assertTrue(hr.getIssues().getIssueList().isEmpty());
  }
}
