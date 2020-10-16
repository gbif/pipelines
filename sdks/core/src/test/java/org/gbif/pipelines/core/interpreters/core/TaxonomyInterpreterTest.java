package org.gbif.pipelines.core.interpreters.core;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.rest.client.species.NameUsageMatch;
import org.gbif.rest.client.species.NameUsageMatch.Diagnostics;
import org.junit.Assert;
import org.junit.Test;

public class TaxonomyInterpreterTest {

  @Test
  public void checkFuzzyPositiveTest() {

    // State
    SpeciesMatchRequest matchRequest =
        SpeciesMatchRequest.builder()
            .withKingdom("")
            .withPhylum("")
            .withClazz("")
            .withOrder("")
            .withFamily("")
            .withGenus("something")
            .build();

    NameUsageMatch usageMatch = new NameUsageMatch();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.FUZZY);
    usageMatch.setDiagnostics(diagnostics);

    // When
    boolean result = TaxonomyInterpreter.checkFuzzy(usageMatch, matchRequest);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void checkFuzzyNegativeTest() {

    // State
    SpeciesMatchRequest matchRequest =
        SpeciesMatchRequest.builder()
            .withKingdom("")
            .withPhylum("")
            .withClazz("")
            .withOrder("")
            .withFamily("something")
            .withGenus("something")
            .build();

    NameUsageMatch usageMatch = new NameUsageMatch();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.FUZZY);
    usageMatch.setDiagnostics(diagnostics);

    // When
    boolean result = TaxonomyInterpreter.checkFuzzy(usageMatch, matchRequest);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void checkFuzzyHighrankTest() {

    // State
    SpeciesMatchRequest matchRequest =
        SpeciesMatchRequest.builder()
            .withKingdom("")
            .withPhylum("")
            .withClazz("")
            .withOrder("")
            .withFamily("")
            .withGenus("something")
            .build();

    NameUsageMatch usageMatch = new NameUsageMatch();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.HIGHERRANK);
    usageMatch.setDiagnostics(diagnostics);

    // When
    boolean result = TaxonomyInterpreter.checkFuzzy(usageMatch, matchRequest);

    // Should
    Assert.assertFalse(result);
  }
}
