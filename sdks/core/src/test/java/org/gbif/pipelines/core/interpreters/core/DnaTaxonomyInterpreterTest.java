package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.model.Constants.NUB_DATASET_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse.Diagnostics;
import org.gbif.rest.client.species.NameUsageMatchResponse.MatchType;
import org.junit.jupiter.api.Test;

class DnaTaxonomyInterpreterTest {

  @Test
  void checkDnaClassificationNoMatchAddsIssue() {
    TaxonRecord testRecord = TaxonRecord.newBuilder().setId("123").build();

    ClassifyOccurrenceResponse.DnaClassification classification =
        new ClassifyOccurrenceResponse.DnaClassification();
    classification.setScientificName(" Panthera leo ");
    classification.setTaxonRank("SPECIES");
    classification.setKingdom("Animalia");
    classification.setPhylum("Chordata");
    classification.setClazz("Mammalia");
    classification.setOrder("Carnivora");
    classification.setFamily("Felidae");
    classification.setGenus("Panthera");

    ClassifyOccurrenceResponse response = new ClassifyOccurrenceResponse();
    response.setGbifID("42");
    response.setClassification(classification);

    DnaTaxonomyInterpreter.dnaTaxonomyInterpreter(
            new KeyValueStore<>() {
              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest request) {
                return null;
              }

              @Override
              public void close() {}
            },
            NUB_DATASET_KEY.toString())
        .accept(response, testRecord);

    assertNotNull(testRecord.getIssues());
    assertTrue(
        testRecord
            .getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.TAXON_MATCH_NONE.toString()));
    assertEquals("42", testRecord.getId());
    assertEquals(NUB_DATASET_KEY.toString(), testRecord.getDatasetKey());
  }

  @Test
  void checkDnaClassificationUsesMatchedTaxonResponse() {
    TaxonRecord testRecord = TaxonRecord.newBuilder().setId("123").build();

    ClassifyOccurrenceResponse.DnaClassification classification =
        new ClassifyOccurrenceResponse.DnaClassification();
    classification.setScientificName(" Panthera leo ");
    classification.setTaxonRank("SPECIES");
    classification.setKingdom("Animalia");
    classification.setPhylum("Chordata");
    classification.setClazz("Mammalia");
    classification.setOrder("Carnivora");
    classification.setFamily("Felidae");
    classification.setGenus("Panthera");

    ClassifyOccurrenceResponse response = new ClassifyOccurrenceResponse();
    response.setGbifID("42");
    response.setClassification(classification);

    NameUsageMatchResponse match = new NameUsageMatchResponse();
    match.setUsage(
        NameUsageMatchResponse.Usage.builder()
            .withName("Panthera leo")
            .withKey("12345")
            .withRank("SPECIES")
            .build());
    match.setClassification(
        java.util.List.of(
            NameUsageMatchResponse.RankedName.builder()
                .withName("Animalia")
                .withKey("1")
                .withRank("KINGDOM")
                .build(),
            NameUsageMatchResponse.RankedName.builder()
                .withName("Panthera leo")
                .withKey("12345")
                .withRank("SPECIES")
                .build()));

    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.EXACT);
    match.setDiagnostics(diagnostics);

    DnaTaxonomyInterpreter.dnaTaxonomyInterpreter(
            new KeyValueStore<>() {
              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest request) {
                return match;
              }

              @Override
              public void close() {}
            },
            NUB_DATASET_KEY.toString())
        .accept(response, testRecord);

    assertNotNull(testRecord.getUsage());
    assertEquals("Panthera leo", testRecord.getUsage().getName());
    assertEquals("12345", testRecord.getUsage().getKey());
    assertFalse(
        testRecord
            .getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.TAXON_MATCH_NONE.toString()));
    assertEquals("42", testRecord.getId());
    assertEquals(NUB_DATASET_KEY.toString(), testRecord.getDatasetKey());
  }

  /** The species of the classifier is sent to the name matching like the other ranks. */
  @Test
  void speciesIsSentToTheNameMatching() {
    ClassifyOccurrenceResponse.DnaClassification classification =
        new ClassifyOccurrenceResponse.DnaClassification();
    classification.setGenus("Panthera");
    classification.setSpecies(" Panthera leo ");

    NameUsageMatchRequest request =
        DnaTaxonomyInterpreter.createNameUsageMatchRequest(
            classification, NUB_DATASET_KEY.toString());

    assertEquals("Panthera", request.getGenus());
    assertEquals("Panthera leo", request.getSpecies());
    assertNull(request.getScientificName());
  }

  /** A classification with only the species has something to match on. */
  @Test
  void onlySpeciesIsEnoughToMatch() {
    ClassifyOccurrenceResponse.DnaClassification classification =
        new ClassifyOccurrenceResponse.DnaClassification();
    classification.setSpecies("Panthera leo");

    NameUsageMatchRequest request =
        DnaTaxonomyInterpreter.createNameUsageMatchRequest(
            classification, NUB_DATASET_KEY.toString());

    assertNotNull(request);
    assertEquals("Panthera leo", request.getSpecies());
  }

  @Test
  void emptyClassificationHasNothingToMatch() {
    assertNull(
        DnaTaxonomyInterpreter.createNameUsageMatchRequest(
            new ClassifyOccurrenceResponse.DnaClassification(), NUB_DATASET_KEY.toString()));
  }

  /** Never called: a blank classification has nothing to look up. */
  private static final KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> NO_CALLS_KVS =
      new KeyValueStore<>() {
        @Override
        public NameUsageMatchResponse get(NameUsageMatchRequest request) {
          throw new AssertionError("The name matching shouldn't be called");
        }

        @Override
        public void close() {}
      };

  private static TaxonRecord interpretBlankClassification(String checklistKey) {
    ClassifyOccurrenceResponse response = new ClassifyOccurrenceResponse();
    response.setGbifID("42");
    response.setClassification(new ClassifyOccurrenceResponse.DnaClassification());
    TaxonRecord taxonRecord = TaxonRecord.newBuilder().setId("42").build();
    DnaTaxonomyInterpreter.dnaTaxonomyInterpreter(NO_CALLS_KVS, checklistKey)
        .accept(response, taxonRecord);
    return taxonRecord;
  }

  /** A blank classification is represented like a name matching without a match. */
  @Test
  void blankClassificationOnTheBackboneIsIncertaeSedis() {
    TaxonRecord taxonRecord = interpretBlankClassification(NUB_DATASET_KEY.toString());

    assertTrue(
        taxonRecord
            .getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.TAXON_MATCH_NONE.toString()));
    assertEquals(TaxonomyInterpreter.INCERTAE_SEDIS_KEY, taxonRecord.getUsage().getKey());
  }

  @Test
  void blankClassificationOnOtherChecklistsHasNoUsage() {
    TaxonRecord taxonRecord =
        interpretBlankClassification(org.gbif.api.model.Constants.COL_DATASET_KEY.toString());

    assertTrue(
        taxonRecord
            .getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.TAXON_MATCH_NONE.toString()));
    assertNull(taxonRecord.getUsage());
    assertTrue(taxonRecord.getClassification().isEmpty());
  }
}
