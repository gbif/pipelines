package org.gbif.pipelines.parsers.parsers.identifier;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gbif.api.vocabulary.UserIdentifierType;
import org.gbif.pipelines.io.avro.UserIdentifier;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UserIdentifierParserTest {

  @Test
  public void parseNullTest() {
    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(null);

    // Should
    assertTrue(set.isEmpty());
  }

  @Test
  public void parseEmptyTest() {
    // State
    String raw = "";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertTrue(set.isEmpty());
  }

  @Test
  public void parseEmptyDelimitrTest() {
    // State
    String raw = "|||";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertTrue(set.isEmpty());
  }

  @Test
  public void parseOrcidTest() {
    // Expected
    Set<UserIdentifier> expected = Collections.singleton(UserIdentifier.newBuilder()
        .setType(UserIdentifierType.ORCID.name())
        .setValue("https://orcid.org/0000-0002-0144-1997")
        .build());

    // State
    String raw = "https://orcid.org/0000-0002-0144-1997";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseOrcidWithoutSchemaTest() {
    // Expected
    Set<UserIdentifier> expected = Collections.singleton(UserIdentifier.newBuilder()
        .setType(UserIdentifierType.ORCID.name())
        .setValue("https://orcid.org/0000-0002-0144-1997")
        .build());

    // State
    String raw = "0000-0002-0144-1997";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseWikidataTest() {
    // Expected
    Set<UserIdentifier> expected = Collections.singleton(UserIdentifier.newBuilder()
        .setType(UserIdentifierType.WIKIDATA.name())
        .setValue("https://www.wikidata.org/wiki/0000")
        .build());

    // State
    String raw = "https://www.wikidata.org/wiki/0000";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseWikidataWithoutSchemaTest() {
    // Expected
    Set<UserIdentifier> expected = Collections.singleton(UserIdentifier.newBuilder()
        .setType(UserIdentifierType.WIKIDATA.name())
        .setValue("https://www.wikidata.org/wiki/0000")
        .build());

    // State
    String raw = "wikidata.org/wiki/0000";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseOtherTest() {
    // Expected
    Set<UserIdentifier> expected = Collections.singleton(UserIdentifier.newBuilder()
        .setType(UserIdentifierType.OTHER.name())
        .setValue("something")
        .build());

    // State
    String raw = "something";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

  @Test
  public void parseMixedTest() {
    // Expected
    Set<UserIdentifier> expected = Stream.of(
        UserIdentifier.newBuilder()
            .setType(UserIdentifierType.ORCID.name())
            .setValue("https://orcid.org/0000-0002-0144-1997")
            .build(),
        UserIdentifier.newBuilder()
            .setType(UserIdentifierType.WIKIDATA.name())
            .setValue("https://www.wikidata.org/wiki/0000")
            .build(),
        UserIdentifier.newBuilder()
            .setType(UserIdentifierType.OTHER.name())
            .setValue("something")
            .build()
    ).collect(Collectors.toSet());

    // State
    String raw = "wikidata.org/wiki/0000| something|0000-0002-0144-1997";

    // When
    Set<UserIdentifier> set = UserIdentifierParser.parse(raw);

    // Should
    assertFalse(set.isEmpty());
    assertEquals(expected, set);
  }

}