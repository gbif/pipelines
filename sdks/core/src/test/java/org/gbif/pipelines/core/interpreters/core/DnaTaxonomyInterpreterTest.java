package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.model.Constants.NUB_DATASET_KEY;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse.Diagnostics;
import org.gbif.rest.client.species.NameUsageMatchResponse.MatchType;
import org.junit.Assert;
import org.junit.Test;

public class DnaTaxonomyInterpreterTest {

  @Test
  public void checkDnaClassificationNoMatchAddsIssue() {
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

    Assert.assertNotNull(testRecord.getIssues());
    Assert.assertTrue(
        testRecord
            .getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.TAXON_MATCH_NONE.toString()));
    Assert.assertEquals("42", testRecord.getId());
    Assert.assertEquals(NUB_DATASET_KEY + "_dna_assisted", testRecord.getDatasetKey());
  }

  @Test
  public void checkDnaClassificationUsesMatchedTaxonResponse() {
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

    Assert.assertNotNull(testRecord.getUsage());
    Assert.assertEquals("Panthera leo", testRecord.getUsage().getName());
    Assert.assertEquals("12345", testRecord.getUsage().getKey());
    Assert.assertFalse(
        testRecord
            .getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.TAXON_MATCH_NONE.toString()));
    Assert.assertEquals("42", testRecord.getId());
    Assert.assertEquals(NUB_DATASET_KEY + "_dna_assisted", testRecord.getDatasetKey());
  }
}
