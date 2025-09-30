package org.gbif.pipelines.core.interpreters.core;

import java.io.IOException;
import java.util.Map;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse.Diagnostics;
import org.gbif.rest.client.species.NameUsageMatchResponse.MatchType;
import org.junit.Assert;
import org.junit.Test;

public class TaxonomyInterpreterTest {

  @Test
  public void checkFuzzyPositiveTest() {

    // State
    NameUsageMatchRequest identification =
        NameUsageMatchRequest.builder()
            .withKingdom("")
            .withPhylum("")
            .withClazz("")
            .withOrder("")
            .withFamily("")
            .withGenus("something")
            .build();

    NameUsageMatchResponse usageMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.VARIANT);
    usageMatch.setDiagnostics(diagnostics);

    // When
    boolean result = TaxonomyInterpreter.checkFuzzy(usageMatch, identification);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void checkFuzzyNegativeTest() {

    // State
    NameUsageMatchRequest identification =
        NameUsageMatchRequest.builder()
            .withKingdom("")
            .withPhylum("")
            .withClazz("")
            .withOrder("")
            .withFamily("something")
            .withGenus("something")
            .build();

    NameUsageMatchResponse usageMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.VARIANT);
    usageMatch.setDiagnostics(diagnostics);

    // When
    boolean result = TaxonomyInterpreter.checkFuzzy(usageMatch, identification);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void checkFuzzyHighrankTest() {

    // State
    NameUsageMatchRequest identification =
        NameUsageMatchRequest.builder()
            .withKingdom("")
            .withPhylum("")
            .withClazz("")
            .withOrder("")
            .withFamily("")
            .withGenus("something")
            .build();

    NameUsageMatchResponse usageMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.HIGHERRANK);
    usageMatch.setDiagnostics(diagnostics);

    // When
    boolean result = TaxonomyInterpreter.checkFuzzy(usageMatch, identification);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void checkNonMatchTest() {

    // State
    NameUsageMatchRequest identification = NameUsageMatchRequest.builder().build();

    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    TaxonRecord testRecord = TaxonRecord.newBuilder().setId("not-an-id").build();

    // When
    TaxonomyInterpreter.taxonomyInterpreter(
            new KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>() {
              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            "fake-checklist-key")
        .accept(
            ExtendedRecord.newBuilder()
                .setId("112345")
                .setCoreTerms(Map.of(DwcTerm.scientificName.name(), "nonsense"))
                .build(),
            testRecord);

    // Should
    Assert.assertNotNull(testRecord.getIssues());
    Assert.assertTrue(
        testRecord
            .getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.TAXON_MATCH_NONE.toString()));
  }
}
