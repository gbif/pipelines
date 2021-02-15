package org.gbif.pipelines.core.parsers.vertnet;

import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.*;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class LengthParserTest {

  @Test
  public void parser0Test() {
    // State
    String value = null;

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void parser1Test() {
    // State
    String value = "{\"totalLengthInMM\":\"123\" };";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("123", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser2Test() {
    // State
    String value =
        "measurements: ToL=230;TaL=115;HF=22;E=18; total length=230 mm; tail length=115 mm;'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("230", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser3Test() {
    // State
    String value = "sex=unknown ; crown-rump length=8 mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void parser4Test() {
    // State
    String value = "left gonad length=10 mm; right gonad length=10 mm;'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void parser5Test() {
    // State
    String value = "\"{\"measurements\":\"308-190-45-20\" }\"";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("308", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser6Test() {
    // State
    String value = "308-190-45-20'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("308", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser7Test() {
    // State
    String value = "{\"measurements\":\"143-63-20-17=13 g\" }'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("143", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser8Test() {
    // State
    String value = "143-63-20-17=13'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("143", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser9Test() {
    // State
    String value = "snout-vent length=54 mm; total length=111 mm; tail length=57 mm; weight=5 g'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("111", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser10Test() {
    // State
    String value =
        "unformatted measurements=Verbatim weight=X;ToL=230;TaL=115;HF=22;E=18;total length=230 mm; tail length=115 mm;'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("230", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser11Test() {
    // State
    String value = "** Body length =345 cm; Blubber=1 cm '";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("head-body length", result.get().getKey());
    Assert.assertEquals("cm", result.get().getType());
    Assert.assertEquals("345", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser12Test() {
    // State
    String value = "t.l.= 2 feet 3.1 - 4.5 inches";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("feet, inches", result.get().getType());
    Assert.assertEquals("2, 3.1 - 4.5", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser13Test() {
    // State
    String value = "2 ft. 3.1 - 4.5 in.";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("2, 3.1 - 4.5", result.get().getValue());
    Assert.assertEquals("ft., in.", result.get().getType());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser14Test() {
    // State
    String value = "total length= 2 ft.'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("ft.", result.get().getType());
    Assert.assertEquals("2", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser15Test() {
    // State
    String value = "AJR-32   186-102-23-15  15.0g'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("186", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser16Test() {
    // State
    String value = "length=8 mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("8", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser17Test() {
    // State
    String value = "another; length=8 mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("8", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser18Test() {
    // State
    String value = "another; TL_120, noise'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("120", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser19Test() {
    // State
    String value = "another; TL - 101.3mm, noise'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("101.3", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser20Test() {
    // State
    String value = "before; TL153, after'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("153", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser21Test() {
    // State
    String value = "before; Total length in catalog and specimen tag as 117, after'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("117", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser22Test() {
    // State
    String value = "before Snout vent lengths range from 16 to 23 mm. after'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("snout-vent length", result.get().getKey());
    Assert.assertEquals("mm.", result.get().getType());
    Assert.assertEquals("16 to 23", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser23Test() {
    // State
    String value = "Size=13 cm TL'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("cm", result.get().getType());
    Assert.assertEquals("13", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser24Test() {
    // State
    String value = "det_comments:31.5-58.3inTL'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("in", result.get().getType());
    Assert.assertEquals("31.5-58.3", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser25Test() {
    // State
    String value = "SVL52mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("snout-vent length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("52", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser26Test() {
    // State
    String value = "snout-vent length=221 mm; total length=257 mm; tail length=36 mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("257", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser27Test() {
    // State
    String value = "SVL 209 mm, total 272 mm, 4.4 g.'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("272", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser28Test() {
    // State
    String value = "{\"time collected\":\"0712-0900\", \"length\":\"12.0\"}";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("12.0", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser29Test() {
    // State
    String value =
        "{\"time collected\":\"1030\", \"water depth\":\"1-8\", \"bottom\":\"abrupt lava cliff dropping off to sand at 45 ft.\", \"length\":\"119-137\"}";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("119-137", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser30Test() {
    // State
    String value = "TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("44", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser31Test() {
    // State
    String value = "{\"totalLengthInMM\":\"270-165-18-22-31\", ";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("270", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser32Test() {
    // State
    String value = "{\"length\":\"20-29\" }";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("20-29", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser33Test() {
    // State
    String value = "field measurements on fresh dead specimen were 157-60-20-19-21g";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("157", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser34Test() {
    // State
    String value = "f age class: adult; standard length: 63-107mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("63-107", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser35Test() {
    // State
    String value = "Rehydrated in acetic acid 7/1978-8/1987.'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void parser36Test() {
    // State
    String value = "age class: adult; standard length: 18.0-21.5mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("18.0-21.5", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser37Test() {
    // State
    String value = "age class: adult; standard length: 18-21.5mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("18-21.5", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser38Test() {
    // State
    String value = "age class: adult; standard length: 18.0-21mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("18.0-21", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser39Test() {
    // State
    String value = "age class: adult; standard length: 18-21mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("18-21", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser40Test() {

    String value =
        "Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,5592,5595,5594,5593,5596,5589,5587,5586,5585";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void parser41Test() {
    // State
    String value = "20-28mm SL'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("20-28", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser42Test() {
    // State
    String value = "29mm SL'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("29", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser43Test() {
    // State
    String value = "{\"measurements\":\"159-?-22-16=21.0\" }";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("159", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser44Test() {
    // State
    String value = "c701563b-dbd9-4500-184f-1ad61eb8da11'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void parser45Test() {
    // State
    String value = "Meas: L: 21.0'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("21.0", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser46Test() {
    // State
    String value = "Meas: L: 21.0 cm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("cm", result.get().getType());
    Assert.assertEquals("21.0", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser47Test() {
    // State
    String value = "LABEL. LENGTH 375 MM.'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm.", result.get().getType());
    Assert.assertEquals("375", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser48Test() {
    // State
    String value = "SL=12mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("12", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser49Test() {
    // State
    String value = "Size=SL 12-14 mm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("standard length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("12-14", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser50Test() {
    // State
    String value = "SV 1.2'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("snout-vent length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("1.2", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser51Test() {
    // State
    String value = " Length: 123 mm SL'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("123", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser52Test() {
    // State
    String value = " Length: 12-34 mmSL'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("mm", result.get().getType());
    Assert.assertEquals("12-34", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser53Test() {
    // State
    String value = "Measurements: L: 21.0 cm'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("total length", result.get().getKey());
    Assert.assertEquals("cm", result.get().getType());
    Assert.assertEquals("21.0", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }

  @Test
  public void parser54Test() {
    // State
    String value = "SVL=44'";

    // When
    Optional<DynamicProperty> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("snout-vent length", result.get().getKey());
    Assert.assertNull(result.get().getType());
    Assert.assertEquals("44", result.get().getValue());
    Assert.assertEquals(Parser.LENGTH, result.get().getField());
  }
}
