package org.gbif.pipelines.core.parsers.dynamic;

import static org.gbif.pipelines.core.parsers.dynamic.MassParser.TOTAL_WEIGHT;

import java.util.Optional;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;
import org.gbif.pipelines.io.avro.DynamicProperty;
import org.junit.Assert;
import org.junit.Test;

public class MassParserTest {

  @Test
  public void testDefualtMap() {
    MassParser.MAIN_TEMPLATE_MAP.forEach((k, v) -> Assert.assertFalse(v.contains("?&")));
  }

  @Test
  public void parser1Test() {
    // State
    String value = "762-292-121-76 2435.0g";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("2435.0", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser2Test() {
    // State
    String value = "TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("0.77", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser3Test() {
    // State
    String value = "Note in catalog: Mus. SW Biol. NK 30009; 91-0-17-22-62g";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("62", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser4Test() {
    // State
    String value = "body mass=20 g";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("20", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser5Test() {
    // State
    String value = "2 lbs. 3.1 - 4.5 oz ";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("lbs., oz", result.get().getType());
    Assert.assertEquals("2, 3.1 - 4.5", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser6Test() {
    // State
    String value =
        "{\"totalLengthInMM\":\"x\", \"earLengthInMM\":\"20\", \"weight\":\"[139.5] g\"}";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("[139.5]", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser7Test() {
    // State
    String value =
        "{\"fat\":\"No fat\", \"gonads\":\"Testes 10 x 6 mm.\", \"molt\":\"No molt\", \"stomach contents\":\"Not recorded\", \"weight\":\"94 gr.\"";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("gr.", result.get().getType());
    Assert.assertEquals("94", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser8Test() {
    // State
    String value = "Note in catalog: 83-0-17-23-fa64-35g";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("35", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser9Test() {
    // State
    String value = "{\"measurements\":\"20.2g, SVL 89.13mm\" }";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("20.2", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser10Test() {
    // State
    String value = "Body: 15 g";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("15", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser11Test() {
    // State
    String value = "82-00-15-21-tr7-fa63-41g";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("41", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser12Test() {
    // State
    String value = "weight=5.4 g; unformatted measurements=77-30-7-12=5.4";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("5.4", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser13Test() {
    // State
    String value = "unformatted measurements=77-30-7-12=5.4; weight=5.4;";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("5.4", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser14Test() {
    // State
    String value = "{\"totalLengthInMM\":\"270-165-18-22-31\", ";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("31", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser15Test() {
    // State
    String value = "{\"measurements\":\"143-63-20-17=13 g\" }";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("13", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser16Test() {
    // State
    String value = "143-63-20-17=13";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("13", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser17Test() {
    // State
    String value =
        "reproductive data: Testes descended -10x7 mm; sex: male; unformatted measurements: 181-75-21-18=22 g";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("22", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser18Test() {
    // State
    String value = "{ \"massingrams\"=\"20.1\" }";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("grams", result.get().getType());
    Assert.assertEquals("20.1", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser19Test() {
    // State
    String value =
        " {\"gonadLengthInMM_1\":\"10\", \"gonadLengthInMM_2\":\"6\", \"weight\":\"1,192.0\" }";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("1,192.0", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser20Test() {
    // State
    String value = "\"weight: 20.5-31.8";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("20.5-31.8", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser21Test() {
    // State
    String value = "\"weight: 20.5-32";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("20.5-32", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser22Test() {
    // State
    String value = "\"weight: 21-31.8";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("21-31.8", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser23Test() {
    // State
    String value = "\"weight: 21-32";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("21-32", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser24Test() {
    // State
    String value =
        "Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,5592,5595,5594,5593,5596,5589,5587,5586,5585";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void parser25Test() {
    // State
    String value = "weight=5.4 g; unformatted measurements=77-x-7-12=5.4";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(TOTAL_WEIGHT, result.get().getKey());
    Assert.assertEquals("g", result.get().getType());
    Assert.assertEquals("5.4", result.get().getValue());
    Assert.assertEquals(Type.STRING, result.get().getClazz());
  }

  @Test
  public void parser26Test() {
    // State
    String value = "c701563b-dbd9-4500-184f-1ad61eb8da11";

    // When
    Optional<DynamicProperty> result = MassParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }
}
