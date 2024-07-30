package org.gbif.pipelines.core.interpreters.core;

import org.gbif.kvs.species.NameUsageMatchRequest;
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
}
