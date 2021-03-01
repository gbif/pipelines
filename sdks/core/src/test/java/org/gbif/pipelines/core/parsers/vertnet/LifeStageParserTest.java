package org.gbif.pipelines.core.parsers.vertnet;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class LifeStageParserTest {

  @Test
  public void lifeStageKeyValueNullTest() {
    // State
    String value = null;

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void lifeStageKeyValueDelimited1Test() {
    // State
    String value = "sex=unknown ; age class=adult/juvenile";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("adult/juvenile", result.get());
  }

  @Test
  public void lifeStageKeyValueDelimited2Test() {
    // State
    String value = "weight=81.00 g; sex=female ? ; age=u ad.";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("u ad.", result.get());
  }

  @Test
  public void lifeStageKeyValueDelimited3Test() {
    // State
    String value = "weight=5.2 g; age class=over-winter ; total length=99 mm;";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("over-winter", result.get());
  }

  @Test
  public void lifeStageKeyValueUndelimited1Test() {
    // State
    String value = "sex=female ? ; age=1st year more than four words here";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("1st year", result.get());
  }

  @Test
  public void lifeStageNoKeyword1Test() {
    // State
    String value = "words after hatching year more words";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("after hatching year", result.get());
  }

  @Test
  public void testExcluded1Test() {
    // State
    String value = "age determined by 20-sided die";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void preferredOrSearch1Test() {
    // State
    String value = "LifeStage Remarks: 5-6 wks";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("5-6 wks", result.get());
  }

  @Test
  public void preferredOrSearch2Test() {
    // State
    String value = "mentions juvenile";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juvenile", result.get());
  }

  @Test
  public void preferredOrSearch3Test() {
    // State
    String value = "mentions juveniles in the field";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juveniles", result.get());
  }

  @Test
  public void preferredOrSearch4Test() {
    // State
    String value = "one or more adults";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("adults", result.get());
  }

  @Test
  public void preferredOrSearch5Test() {
    // State
    String value = "adults";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("adults", result.get());
  }

  @Test
  public void preferredOrSearch6Test() {
    // State
    String value = "adult";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("adult", result.get());
  }

  @Test
  public void preferredOrSearch7Test() {
    // State
    String value = "Adulte";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("adulte", result.get());
  }

  @Test
  public void preferredOrSearch8Test() {
    // State
    String value = "AGE IMM";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("imm", result.get());
  }

  @Test
  public void preferredOrSearch9Test() {
    // State
    String value = "subadult";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("subadult", result.get());
  }

  @Test
  public void preferredOrSearch10Test() {
    // State
    String value = "subadults";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("subadults", result.get());
  }

  @Test
  public void preferredOrSearch11Test() {
    // State
    String value = "subadultery";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void preferredOrSearch12Test() {
    // State
    String value = "in which larvae are found";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("larvae", result.get());
  }

  @Test
  public void preferredOrSearch13Test() {
    // State
    String value = "larval";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("larval", result.get());
  }

  @Test
  public void preferredOrSearch14Test() {
    // State
    String value = "solitary larva, lonely";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("larva", result.get());
  }

  @Test
  public void preferredOrSearch15Test() {
    // State
    String value = "juvénile";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juvénile", result.get());
  }

  @Test
  public void preferredOrSearch16Test() {
    // State
    String value = "Têtard";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("têtard", result.get());
  }

  @Test
  public void preferredOrSearch17Test() {
    // State
    String value = "what if it is a subad.?";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("subad", result.get());
  }

  @Test
  public void preferredOrSearch18Test() {
    // State
    String value = "subad is a possibility";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("subad", result.get());
  }

  @Test
  public void preferredOrSearch19Test() {
    // State
    String value = "one tadpole";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("tadpole", result.get());
  }

  @Test
  public void preferredOrSearch20Test() {
    // State
    String value = "two tadpoles";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("tadpoles", result.get());
  }

  @Test
  public void preferredOrSearch21Test() {
    // State
    String value = "an ad.";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("ad", result.get());
  }

  @Test
  public void preferredOrSearch22Test() {
    // State
    String value = "what about ad";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("ad", result.get());
  }

  @Test
  public void preferredOrSearch23Test() {
    // State
    String value = "ad. is a possibility";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("ad", result.get());
  }

  @Test
  public void preferredOrSearch24Test() {
    // State
    String value = "ad is also a possibility";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("ad", result.get());
  }

  @Test
  public void preferredOrSearch25Test() {
    // State
    String value = "embryonic";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void preferredOrSearch26Test() {
    // State
    String value = "IMM";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("imm", result.get());
  }

  @Test
  public void preferredOrSearch27Test() {
    // State
    String value = "immature";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("immature", result.get());
  }

  @Test
  public void preferredOrSearch28Test() {
    // State
    String value = "immatures";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("immatures", result.get());
  }

  @Test
  public void preferredOrSearch29Test() {
    // State
    String value = "imm.";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("imm", result.get());
  }

  @Test
  public void preferredOrSearch30Test() {
    // State
    String value = "juv.";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juv", result.get());
  }

  @Test
  public void preferredOrSearch31Test() {
    // State
    String value = "one juv to rule them all";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juv", result.get());
  }

  @Test
  public void preferredOrSearch32Test() {
    // State
    String value = "how many juvs does it take?";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juvs", result.get());
  }

  @Test
  public void preferredOrSearch33Test() {
    // State
    String value = "juvs.?";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juvs", result.get());
  }

  @Test
  public void preferredOrSearch34Test() {
    // State
    String value = "juvenile(s)";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juvenile", result.get());
  }

  @Test
  public void preferredOrSearch35Test() {
    // State
    String value = "larva(e)";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("larva", result.get());
  }

  @Test
  public void preferredOrSearch36Test() {
    // State
    String value = "young";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("young", result.get());
  }

  @Test
  public void preferredOrSearch37Test() {
    // State
    String value = "young adult";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("young", result.get());
  }

  @Test
  public void preferredOrSearch38Test() {
    // State
    String value = "adult young";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("adult", result.get());
  }

  @Test
  public void preferredOrSearch39Test() {
    // State
    String value = "sub-adult";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("sub-adult", result.get());
  }

  @Test
  public void preferredOrSearch40Test() {
    // State
    String value = "hatched";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("hatched", result.get());
  }

  @Test
  public void preferredOrSearch41Test() {
    // State
    String value = "'adult(s) and juvenile(s)";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("adult", result.get());
  }

  @Test
  public void preferredOrSearch42Test() {
    // State
    String value = "juvenile(s) and adult(s)";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("juvenile", result.get());
  }

  @Test
  public void preferredOrSearch43Test() {
    // State
    String value = "young-of-the-year";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("young-of-the-year", result.get());
  }

  @Test
  public void preferredOrSearch44Test() {
    // State
    String value = "YOLK SAC";

    // When
    Optional<String> result = LifeStageParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("yolk sac", result.get());
  }
}
