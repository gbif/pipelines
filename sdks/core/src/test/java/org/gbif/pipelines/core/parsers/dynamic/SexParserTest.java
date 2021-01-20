package org.gbif.pipelines.core.parsers.dynamic;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class SexParserTest {

  @Test
  public void sexKeyValueDelimited1Test() {
    // State
    String value = "weight=81.00 g; sex=female ? ; age=u ad.";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("female ?", result.get());
  }

  @Test
  public void sexKeyValueDelimited2Test() {
    // State
    String value = "sex=unknown ; crown-rump length=8 mm";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("unknown", result.get());
  }

  @Test
  public void sexKeyValueUndelimited1Test() {
    // State
    String value = "sex=F crown rump length=8 mm";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("F", result.get());
  }

  @Test
  public void sexUnkeyed1Test() {
    // State
    String value = "words male female unknown more words";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("male/female", result.get());
  }

  @Test
  public void sexUnkeyed2Test() {
    // State
    String value = "words male female male more words";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void sexUnkeyed3Test() {
    // State
    String value = "";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void excluded1Test() {
    // State
    String value = "Respective sex and msmt. in mm";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void preferredOrSearch1Test() {
    // State
    String value = "mention MALE in a phrase";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("male", result.get());
  }

  @Test
  public void preferredOrSearch2Test() {
    // State
    String value = "MALE in a phrase";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("male", result.get());
  }

  @Test
  public void preferredOrSearch3Test() {
    // State
    String value = "male or female";

    // When
    Optional<String> result = SexParser.parseSex(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("male,female", result.get());
  }
}
