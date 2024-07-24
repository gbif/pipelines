package org.gbif.pipelines.core.interpreters.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.MockVocabularyLookups;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class GeologicalContextInterpreterTest {

  private static final String ID = "777";

  private final VocabularyService vocabularyService =
      VocabularyService.builder()
          .vocabularyLookup(
              DwcTerm.GROUP_GEOLOGICALCONTEXT,
              new MockVocabularyLookups.GeoTimeMockVocabularyLookup())
          .build();

  @Test
  public void interpretChronostratigraphyTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.earliestPeriodOrLowestSystem.qualifiedName(), "neogene");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    GeologicalContextInterpreter.interpretChronostratigraphy(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(
        "Neogene", br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getConcept());
    Assert.assertEquals(
        Arrays.asList("Cenozoic", "Neogene"),
        br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getLineage());
    Assert.assertEquals(
        3, br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getTags().size());
  }

  @Test
  public void interpretChronostratigraphyInferredAndRankMismatchTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.earliestPeriodOrLowestSystem.qualifiedName(), "miocene");
    coreMap.put(DwcTerm.earliestEraOrLowestErathem.qualifiedName(), "miocene");
    coreMap.put(DwcTerm.latestPeriodOrHighestSystem.qualifiedName(), "cenozoic");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    GeologicalContextInterpreter.interpretChronostratigraphy(vocabularyService).accept(er, br);

    // Should
    Assert.assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.ERA_OR_ERATHEM_INFERRED_FROM_PARENT_RANK.name()));
    Assert.assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.PERIOD_OR_SYSTEM_RANK_MISMATCH.name()));
    Assert.assertEquals(
        "Neogene", br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getConcept());
    Assert.assertEquals(
        Arrays.asList("Cenozoic", "Neogene"),
        br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getLineage());
    Assert.assertEquals(
        3, br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getTags().size());
    Assert.assertEquals(
        "Cenozoic", br.getGeologicalContext().getEarliestEraOrLowestErathem().getConcept());
    Assert.assertEquals(
        Collections.singletonList("Cenozoic"),
        br.getGeologicalContext().getEarliestEraOrLowestErathem().getLineage());
    Assert.assertEquals(
        3, br.getGeologicalContext().getEarliestEraOrLowestErathem().getTags().size());
    Assert.assertNull(br.getGeologicalContext().getLatestPeriodOrHighestSystem());
  }

  @Test
  public void interpretChronostratigraphyInvalidRangeTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.earliestPeriodOrLowestSystem.qualifiedName(), "quaternary");
    coreMap.put(DwcTerm.latestPeriodOrHighestSystem.qualifiedName(), "neogene");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    GeologicalContextInterpreter.interpretChronostratigraphy(vocabularyService).accept(er, br);

    // Should
    Assert.assertNull(br.getGeologicalContext().getEarliestPeriodOrLowestSystem());
    Assert.assertNull(br.getGeologicalContext().getLatestPeriodOrHighestSystem());
    Assert.assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.PERIOD_OR_SYSTEM_INVALID_RANGE.name()));
  }

  @Test
  public void interpretChronostratigraphyParentMismatchTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.earliestEraOrLowestErathem.qualifiedName(), "paleozoic");
    coreMap.put(DwcTerm.latestEraOrHighestErathem.qualifiedName(), "paleozoic");
    coreMap.put(DwcTerm.earliestPeriodOrLowestSystem.qualifiedName(), "neogene");
    coreMap.put(DwcTerm.latestPeriodOrHighestSystem.qualifiedName(), "quaternary");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    GeologicalContextInterpreter.interpretChronostratigraphy(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(
        "Neogene", br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getConcept());
    Assert.assertEquals(
        "Quaternary", br.getGeologicalContext().getLatestPeriodOrHighestSystem().getConcept());
    Assert.assertEquals(
        "Paleozoic", br.getGeologicalContext().getEarliestEraOrLowestErathem().getConcept());
    Assert.assertEquals(
        "Paleozoic", br.getGeologicalContext().getLatestEraOrHighestErathem().getConcept());
    Assert.assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.ERA_OR_ERATHEM_AND_PERIOD_OR_SYSTEM_MISMATCH.name()));
  }

  @Test
  public void interpretChronostratigraphyIncompleteFieldsTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.earliestEraOrLowestErathem.qualifiedName(), "paleozoic");
    coreMap.put(DwcTerm.earliestPeriodOrLowestSystem.qualifiedName(), "neogene");
    coreMap.put(DwcTerm.latestPeriodOrHighestSystem.qualifiedName(), "quaternary");
    coreMap.put(DwcTerm.earliestAgeOrLowestStage.qualifiedName(), "burdigalian");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    GeologicalContextInterpreter.interpretChronostratigraphy(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(
        "Neogene", br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getConcept());
    Assert.assertEquals(
        "Quaternary", br.getGeologicalContext().getLatestPeriodOrHighestSystem().getConcept());
    Assert.assertEquals(
        "Paleozoic", br.getGeologicalContext().getEarliestEraOrLowestErathem().getConcept());
    Assert.assertNull(br.getGeologicalContext().getLatestEraOrHighestErathem());
    Assert.assertEquals(
        "Burdigalian", br.getGeologicalContext().getEarliestAgeOrLowestStage().getConcept());
    Assert.assertNull(br.getGeologicalContext().getLatestAgeOrHighestStage());
    Assert.assertEquals(
        Arrays.asList("Cenozoic", "Neogene", "Miocene", "Burdigalian"),
        br.getGeologicalContext().getEarliestAgeOrLowestStage().getLineage());
    Assert.assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.ERA_OR_ERATHEM_AND_PERIOD_OR_SYSTEM_MISMATCH.name()));
  }

  @Test
  public void interpretChronostratigraphyWiderRangeTest() {
    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.earliestEraOrLowestErathem.qualifiedName(), "paleozoic");
    coreMap.put(DwcTerm.latestEraOrHighestErathem.qualifiedName(), "cenozoic");
    coreMap.put(DwcTerm.earliestPeriodOrLowestSystem.qualifiedName(), "neogene");
    coreMap.put(DwcTerm.latestPeriodOrHighestSystem.qualifiedName(), "quaternary");
    coreMap.put(DwcTerm.latestEpochOrHighestSeries.qualifiedName(), "miocene");
    coreMap.put(DwcTerm.latestAgeOrHighestStage.qualifiedName(), "zanclean");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    GeologicalContextInterpreter.interpretChronostratigraphy(vocabularyService).accept(er, br);

    // Should
    Assert.assertEquals(
        "Neogene", br.getGeologicalContext().getEarliestPeriodOrLowestSystem().getConcept());
    Assert.assertEquals(
        "Quaternary", br.getGeologicalContext().getLatestPeriodOrHighestSystem().getConcept());
    Assert.assertEquals(
        "Miocene", br.getGeologicalContext().getLatestEpochOrHighestSeries().getConcept());
    Assert.assertEquals(
        "Paleozoic", br.getGeologicalContext().getEarliestEraOrLowestErathem().getConcept());
    Assert.assertEquals(
        "Cenozoic", br.getGeologicalContext().getLatestEraOrHighestErathem().getConcept());
    Assert.assertEquals(
        "Zanclean", br.getGeologicalContext().getLatestAgeOrHighestStage().getConcept());
    Assert.assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.EPOCH_OR_SERIES_AND_AGE_OR_STAGE_MISMATCH.name()));
  }
}
