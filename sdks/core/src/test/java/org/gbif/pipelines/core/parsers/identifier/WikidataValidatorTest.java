package org.gbif.pipelines.core.parsers.identifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class WikidataValidatorTest {

  @Test
  public void isValidTest() {

    // State
    WikidataValidator validator = new WikidataValidator();

    // Should
    assertTrue(validator.isValid("https://www.wikidata.org/wiki/Q12345"));
    assertTrue(validator.isValid("https://www.wikidata.org/wiki/Property:P569"));
    assertTrue(validator.isValid("https://www.wikidata.org/wiki/Lexeme:L1"));
    assertTrue(validator.isValid("http://www.wikidata.org/wiki/Lexeme:L1"));
    assertTrue(validator.isValid("http://www.wikidata.org/wiki/Property:P569"));
    assertTrue(validator.isValid("http://www.wikidata.org/entity/ID"));
    assertTrue(validator.isValid("wikidata.org/wiki/Property:P569"));
    assertTrue(validator.isValid("www.wikidata.org/wiki/Lexeme:L1"));
    assertTrue(validator.isValid("www.wikidata.org/entity/ID"));

    assertFalse(validator.isValid(null));
    assertFalse(validator.isValid(""));
    assertFalse(validator.isValid("http.wikidata.org/entity/ID"));
    assertFalse(validator.isValid("ftp://www.wikidata.org/entity/ID"));
    assertFalse(validator.isValid("http://www.wikidata.org/entity/ID/awdawdawd"));
    assertFalse(validator.isValid("https://www.wikidata.org/wiki/Lexeme:L1/awdawd"));
    assertFalse(validator.isValid("https://www.wikidata.org/wiki/"));
    assertFalse(validator.isValid("https://www.ad.com/#q=ad"));
    assertFalse(validator.isValid("http://www.wikidata.org"));
    assertFalse(validator.isValid("https://www.wikidata.org"));
    assertFalse(validator.isValid("awdawdawd"));
  }

  @Test
  public void normalizeTest() {

    // State
    WikidataValidator validator = new WikidataValidator();

    // Should
    assertEquals(
        "https://www.wikidata.org/wiki/Q12345",
        validator.normalize("https://www.wikidata.org/wiki/Q12345"));
    assertEquals(
        "https://www.wikidata.org/wiki/Property:P569",
        validator.normalize("https://www.wikidata.org/wiki/Property:P569"));
    assertEquals(
        "https://www.wikidata.org/wiki/Lexeme:L1",
        validator.normalize("https://www.wikidata.org/wiki/Lexeme:L1"));
    assertEquals(
        "http://www.wikidata.org/wiki/Lexeme:L1",
        validator.normalize("http://www.wikidata.org/wiki/Lexeme:L1"));
    assertEquals(
        "http://www.wikidata.org/wiki/Property:P569",
        validator.normalize("http://www.wikidata.org/wiki/Property:P569"));
    assertEquals(
        "http://www.wikidata.org/entity/ID",
        validator.normalize("http://www.wikidata.org/entity/ID"));
    assertEquals(
        "wikidata.org/wiki/Property:P569", validator.normalize("wikidata.org/wiki/Property:P569"));
    assertEquals(
        "www.wikidata.org/wiki/Lexeme:L1", validator.normalize("www.wikidata.org/wiki/Lexeme:L1"));
    assertEquals("www.wikidata.org/entity/ID", validator.normalize("www.wikidata.org/entity/ID"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void normalizExeptionTest() {

    // State
    WikidataValidator validator = new WikidataValidator();

    // Should
    validator.normalize("awdawd");
  }
}
