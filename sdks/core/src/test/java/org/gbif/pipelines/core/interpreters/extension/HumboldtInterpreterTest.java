package org.gbif.pipelines.core.interpreters.extension;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.vocabulary.DurationUnit;
import org.gbif.api.vocabulary.EventIssue;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.MockVocabularyLookups;
import org.gbif.pipelines.core.interpreters.NameUsageMatchKeyValueTestStore;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.junit.Test;

public class HumboldtInterpreterTest {

  private static final String DEFAULT_CHECKLIST_KEY = UUID.randomUUID().toString();

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
    humboldtData.put("http://rs.tdwg.org/eco/terms/hasNonTargetTaxa", "False");
    humboldtData.put("http://rs.tdwg.org/eco/terms/nonTargetTaxa", "aves");
    humboldtData.put(
        "http://rs.tdwg.org/eco/terms/isTaxonomicScopeFullyReported", "True in some cases");
    humboldtData.put("http://rs.tdwg.org/eco/terms/targetLifeStageScope", "ADUlt");
    humboldtData.put("http://rs.tdwg.org/eco/terms/targetDegreeOfEstablishmentScope", "td1| td2");
    humboldtData.put("http://rs.tdwg.org/eco/terms/excludedDegreeOfEstablishmentScope", "td1");
    humboldtData.put("http://rs.tdwg.org/eco/terms/targetTaxonomicScope", "none | aves");

    ext.put("http://rs.tdwg.org/eco/terms/Event", List.of(humboldtData));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    HumboldtRecord hr = HumboldtRecord.newBuilder().setId("id").build();

    // When
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
        .kvStore(new NameUsageMatchKeyValueTestStore())
        .checklistKeys(List.of(DEFAULT_CHECKLIST_KEY))
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
    assertEquals(DurationUnit.MINUTES.name(), humboldt.getEventDurationUnit());
    assertNull(humboldt.getIsTaxonomicScopeFullyReported());
    assertEquals(1, humboldt.getTargetLifeStageScope().size());
    assertEquals("Adult", humboldt.getTargetLifeStageScope().get(0).getConcept());
    assertEquals(2, humboldt.getTargetDegreeOfEstablishmentScope().size());
    assertTrue(
        humboldt.getTargetDegreeOfEstablishmentScope().stream()
            .anyMatch(c -> c.getConcept().equals("td1")));
    assertEquals(1, humboldt.getExcludedDegreeOfEstablishmentScope().size());
    assertEquals(2, humboldt.getTargetTaxonomicScope().size());
    assertEquals(
        2,
        humboldt.getTargetTaxonomicScope().stream()
            .filter(ts -> ts.getChecklistKey().equals(DEFAULT_CHECKLIST_KEY))
            .count());
    assertEquals(
        1,
        humboldt.getTargetTaxonomicScope().stream()
            .filter(
                ts ->
                    ts.getClassification().size() == 2
                        && ts.getUsageName().equalsIgnoreCase("Aves")
                        && ts.getUsageRank() != null
                        && ts.getUsageKey() != null)
            .count());
    assertEquals(
        1,
        humboldt.getTargetTaxonomicScope().stream()
            .filter(
                ts ->
                    ts.getClassification().size() == 1
                        && ts.getUsageName().equals(TaxonomyInterpreter.INCERTAE_SEDIS_NAME)
                        && ts.getUsageKey().equals(TaxonomyInterpreter.INCERTAE_SEDIS_KEY)
                        && ts.getUsageRank().equals(TaxonomyInterpreter.KINGDOM_RANK)
                        && ts.getIssues()
                            .getIssueList()
                            .contains(OccurrenceIssue.TAXON_MATCH_NONE.name()))
            .count());
    assertFalse(hr.getIssues().getIssueList().contains(OccurrenceIssue.TAXON_MATCH_NONE.name()));

    assertTrue(
        hr.getIssues()
            .getIssueList()
            .contains(EventIssue.GEOSPATIAL_SCOPE_AREA_LOWER_THAN_TOTAL_AREA_SAMPLED.name()));
    assertTrue(hr.getIssues().getIssueList().contains(EventIssue.SITE_COUNT_INVALID.name()));
    assertTrue(
        hr.getIssues().getIssueList().contains(EventIssue.SAMPLING_EFFORT_UNIT_MISSING.name()));
    assertTrue(
        hr.getIssues()
            .getIssueList()
            .contains(EventIssue.IS_TAXONOMIC_SCOPE_FULLY_REPORTED_INVALID.name()));
    assertTrue(
        hr.getIssues()
            .getIssueList()
            .contains(EventIssue.TARGET_DEGREE_OF_ESTABLISHMENT_EXCLUDED.name()));
    assertTrue(
        hr.getIssues().getIssueList().contains(EventIssue.HAS_NON_TARGET_TAXA_MISMATCH.name()));
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
            .contains(EventIssue.GEOSPATIAL_SCOPE_AREA_UNIT_MISSING.name()));
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
