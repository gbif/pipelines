package org.gbif.pipelines.core.parsers.identifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.junit.Test;

public class AgentIdentifierParserTest {

  @Test
  public void parseNullTest() {
    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(null);

    // Should
    assertTrue(set.isEmpty());
  }

  @Test
  public void parseEmptyTest() {
    // State
    String raw = "";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertTrue(set.isEmpty());
  }

  @Test
  public void parseEmptyDelimitrTest() {
    // State
    String raw = "|||";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertTrue(set.isEmpty());
  }

  @Test
  public void parseOrcidTest() {
    // Expected
    Set<AgentIdentifier> expected =
        Collections.singleton(
            AgentIdentifier.newBuilder()
                .setType(AgentIdentifierType.ORCID.name())
                .setValue("https://orcid.org/0000-0002-0144-1997")
                .build());

    // State
    String raw = "https://orcid.org/0000-0002-0144-1997";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseOrcidWithoutSchemaTest() {
    // Expected
    Set<AgentIdentifier> expected =
        Collections.singleton(
            AgentIdentifier.newBuilder()
                .setType(AgentIdentifierType.ORCID.name())
                .setValue("https://orcid.org/0000-0002-0144-1997")
                .build());

    // State
    String raw = "0000-0002-0144-1997";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseWikidataTest() {
    // Expected
    Set<AgentIdentifier> expected =
        Collections.singleton(
            AgentIdentifier.newBuilder()
                .setType(AgentIdentifierType.WIKIDATA.name())
                .setValue("https://www.wikidata.org/wiki/0000")
                .build());

    // State
    String raw = "https://www.wikidata.org/wiki/0000";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseWikidataWithoutSchemaTest() {
    // Expected
    Set<AgentIdentifier> expected =
        Collections.singleton(
            AgentIdentifier.newBuilder()
                .setType(AgentIdentifierType.WIKIDATA.name())
                .setValue("wikidata.org/wiki/0000")
                .build());

    // State
    String raw = "wikidata.org/wiki/0000";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseOtherTest() {
    // Expected
    Set<AgentIdentifier> expected =
        Collections.singleton(
            AgentIdentifier.newBuilder()
                .setType(AgentIdentifierType.OTHER.name())
                .setValue("something")
                .build());

    // State
    String raw = "something";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseMixedTest() {
    // Expected
    Set<AgentIdentifier> expected =
        Stream.of(
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.ORCID.name())
                    .setValue("https://orcid.org/0000-0002-0144-1997")
                    .build(),
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.WIKIDATA.name())
                    .setValue("wikidata.org/wiki/0000")
                    .build(),
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.OTHER.name())
                    .setValue("something")
                    .build())
            .collect(Collectors.toSet());

    // State
    String raw = "wikidata.org/wiki/0000| something|0000-0002-0144-1997";

    // When
    Set<AgentIdentifier> set = AgentIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }
}
