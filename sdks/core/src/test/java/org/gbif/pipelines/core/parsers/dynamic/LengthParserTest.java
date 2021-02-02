package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class LengthParserTest {

  @Test
  public void parser1Test() {
    // State
    String value = "{\"totalLengthInMM\":\"123\" };";
    String expected = "key': 'total length', 'value': '230', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser2Test() {
    // State
    String value =
        "measurements: ToL=230;TaL=115;HF=22;E=18; total length=230 mm; tail length=115 mm;'";
    String expected = "key': 'total length', 'value': '230', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser3Test() {
    // State
    String value = "sex=unknown ; crown-rump length=8 mm'";
    String expected = null;

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser4Test() {
    // State
    String value = "left gonad length=10 mm; right gonad length=10 mm;'";
    String expected = null;

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser5Test() {
    // State
    String value = "\"{\"measurements\":\"308-190-45-20\" }\"";
    String expected = "key': 'measurements', 'value': '308', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser6Test() {
    // State
    String value = "308-190-45-20'";
    String expected = "key': '_shorthand_', 'value': '308', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser7Test() {
    // State
    String value = "{\"measurements\":\"143-63-20-17=13 g\" }'";
    String expected = "key': 'measurements', 'value': '143', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser8Test() {
    // State
    String value = "143-63-20-17=13'";
    String expected = "key': '_shorthand_', 'value': '143', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser9Test() {
    // State
    String value = "snout-vent length=54 mm; total length=111 mm; tail length=57 mm; weight=5 g'";
    String expected = "key': 'total length', 'value': '111', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser10Test() {
    // State
    String value =
        "unformatted measurements=Verbatim weight=X;ToL=230;TaL=115;HF=22;E=18;total length=230 mm; tail length=115 mm;'";
    String expected = "key': 'total length', 'value': '230', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser11Test() {
    // State
    String value = "** Body length =345 cm; Blubber=1 cm '";
    String expected = "key': 'Body length', 'value': '345', 'units': 'cm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser12Test() {
    // State
    String value = "t.l.= 2 feet 3.1 - 4.5 inches";
    String expected = "key': 't.l.', 'value': ['2', '3.1 - 4.5'], 'units': ['feet', 'inches";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser13Test() {
    // State
    String value = "2 ft. 3.1 - 4.5 in.";
    String expected = "key': '_english_', 'value': ['2', '3.1 - 4.5'], 'units': ['ft.', 'in.";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser14Test() {
    // State
    String value = "total length= 2 ft.'";
    String expected = "key': 'total length', 'value': '2', 'units': 'ft.'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser15Test() {
    // State
    String value = "AJR-32   186-102-23-15  15.0g'";
    String expected = "key': '_shorthand_', 'value': '186', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser16Test() {
    // State
    String value = "length=8 mm'";
    String expected = "key': 'length', 'value': '8', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser17Test() {
    // State
    String value = "another; length=8 mm'";
    String expected = "key': 'length', 'value': '8', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser18Test() {
    // State
    String value = "another; TL_120, noise'";
    String expected = "key': 'TL_', 'value': '120', 'units': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser19Test() {
    // State
    String value = "another; TL - 101.3mm, noise'";
    String expected = "key': 'TL', 'value': '101.3', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser20Test() {
    // State
    String value = "before; TL153, after'";
    String expected = "key': 'TL', 'value': '153', 'units': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser21Test() {
    // State
    String value = "before; Total length in catalog and specimen tag as 117, after'";
    String expected = "key': 'Total length', 'value': '117', 'units': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser22Test() {
    // State
    String value = "before Snout vent lengths range from 16 to 23 mm. after'";
    String expected = "key': 'Snout vent lengths', 'value': '16 to 23', 'units': 'mm.'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser23Test() {
    // State
    String value = "Size=13 cm TL'";
    String expected = "key': 'TL', 'value': '13', 'units': 'cm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser24Test() {
    // State
    String value = "det_comments:31.5-58.3inTL'";
    String expected = "key': 'TL', 'value': '31.5-58.3', 'units': 'in'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser25Test() {
    // State
    String value = "SVL52mm'";
    String expected = "key': 'SVL', 'value': '52', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser26Test() {
    // State
    String value = "snout-vent length=221 mm; total length=257 mm; tail length=36 mm'";
    String expected = "key': 'total length', 'value': '257', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser27Test() {
    // State
    String value = "SVL 209 mm, total 272 mm, 4.4 g.'";
    String expected = "key': 'total', 'value': '272', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser28Test() {
    // State
    String value = "{\"time collected\":\"0712-0900\", \"length\":\"12.0\"}";
    String expected = "key': 'length', 'value': '12.0', 'units': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser29Test() {
    // State
    String value =
        "{\"time collected\":\"1030\", \"water depth\":\"1-8\", \"bottom\":\"abrupt lava cliff dropping off to sand at 45 ft.\", \"length\":\"119-137\"}";
    String expected = "key': 'length', 'value': '119-137', 'units': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser30Test() {
    // State
    String value = "TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx'";
    String expected = "key': 'TL', 'value': '44', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser31Test() {
    // State
    String value = "{\"totalLengthInMM\":\"270-165-18-22-31\", ";
    String expected = "key': 'totalLengthInMM', 'value': '270', 'units': 'MM'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser32Test() {
    // State
    String value = "{\"length\":\"20-29\" }";
    String expected = "key': 'length', 'value': '20-29', 'units': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser33Test() {
    // State
    String value = "field measurements on fresh dead specimen were 157-60-20-19-21g";
    String expected = "key': '_shorthand_', 'value': '157', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser34Test() {
    // State
    String value = "f age class: adult; standard length: 63-107mm'";
    String expected = "key': 'standard length', 'value': '63-107', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser35Test() {
    // State
    String value = "Rehydrated in acetic acid 7/1978-8/1987.'";
    String expected = null;

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser36Test() {
    // State
    String value = "age class: adult; standard length: 18.0-21.5mm'";
    String expected = "key': 'standard length', 'value': '18.0-21.5', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser37Test() {
    // State
    String value = "age class: adult; standard length: 18-21.5mm'";
    String expected = "key': 'standard length', 'value': '18-21.5', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser38Test() {
    // State
    String value = "age class: adult; standard length: 18.0-21mm'";
    String expected = "key': 'standard length', 'value': '18.0-21', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser39Test() {
    // State
    String value = "age class: adult; standard length: 18-21mm'";
    String expected = "key': 'standard length', 'value': '18-21', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser40Test() {

    String value =
        "Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,5592,5595,5594,5593,5596,5589,5587,5586,5585";
    String expected = null;

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser41Test() {
    // State
    String value = "20-28mm SL'";
    String expected = "key': 'SL', 'value': '20-28', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser42Test() {
    // State
    String value = "29mm SL'";
    String expected = "key': 'SL', 'value': '29', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser43Test() {
    // State
    String value = "{\"measurements\":\"159-?-22-16=21.0\" }";
    String expected = "key': 'measurements', 'value': '159', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser44Test() {
    // State
    String value = "c701563b-dbd9-4500-184f-1ad61eb8da11'";
    String expected = null;

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser45Test() {
    // State
    String value = "Meas: L: 21.0'";
    String expected = "key': 'Meas: L', 'value': '21.0', 'units': '_mm_'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser46Test() {
    // State
    String value = "Meas: L: 21.0 cm'";
    String expected = "key': 'Meas: L', 'value': '21.0', 'units': 'cm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser47Test() {
    // State
    String value = "LABEL. LENGTH 375 MM.'";
    String expected = "key': 'LABEL. LENGTH', 'value': '375', 'units': 'MM.'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser48Test() {
    // State
    String value = "SL=12mm'";
    String expected = "key': 'SL', 'value': '12', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser49Test() {
    // State
    String value = "Size=SL 12-14 mm'";
    String expected = "key': 'SL', 'value': '12-14', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser50Test() {
    // State
    String value = "SV 1.2'";
    String expected = "key': 'SV', 'value': '1.2', 'units': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser51Test() {
    // State
    String value = " Length: 123 mm SL'";
    String expected = "key': 'SL', 'value': '123', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser52Test() {
    // State
    String value = " Length: 12-34 mmSL'";
    String expected = "key': 'SL', 'value': '12-34', 'units': 'mm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser53Test() {
    // State
    String value = "Measurements: L: 21.0 cm'";
    String expected = "key': 'Measurements: L', 'value': '21.0', 'units': 'cm'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser54Test() {
    // State
    String value = "SVL=44'";
    String expected = "key': 'SVL', 'value': '44', 'units': None";

    ///////////////////////////

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize1Test() {
    // State
    String value = "{\"totalLengthInMM\":\"123\" };";
    String expected =
        "haslength': 1, 'lengthinmm': 123, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize2Test() {
    // State
    String value =
        "measurements: ToL=230;TaL=115;HF=22;E=18; total length=230 mm; tail length=115 mm;";
    String expected =
        "haslength': 1, 'lengthinmm': 230, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize3Test() {
    // State
    String value = "sex=unknown ; crown-rump length=8 mm";
    String expected =
        "haslength': 0, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize4Test() {
    // State
    String value = "left gonad length=10 mm; right gonad length=10 mm;";
    String expected =
        "haslength': 0, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize5Test() {
    // State
    String value = "\"{\"measurements\":\"308-190-45-20\" }\"";
    String expected =
        "haslength': 1, 'lengthinmm': 308, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize6Test() {
    // State
    String value = "308-190-45-20";
    String expected =
        "haslength': 1, 'lengthinmm': 308, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize7Test() {
    // State
    String value = "{\"measurements\":\"143-63-20-17=13 g\" }";
    String expected =
        "haslength': 1, 'lengthinmm': 143, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize8Test() {
    // State
    String value = "143-63-20-17=13";
    String expected =
        "haslength': 1, 'lengthinmm': 143, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize9Test() {
    // State
    String value = "snout-vent length=54 mm; total length=111 mm; tail length=57 mm; weight=5 g";
    String expected =
        "haslength': 1, 'lengthinmm': 111, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize10Test() {
    // State
    String value =
        "unformatted measurements=Verbatim weight=X;ToL=230;TaL=115;HF=22;E=18; total length=230 mm; tail length=115 mm;";
    String expected =
        "haslength': 1, 'lengthinmm': 230, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize11Test() {
    // State
    String value = "** Body length =345 cm; Blubber=1 cm ";
    String expected =
        "haslength': 1, 'lengthinmm': 3450, 'lengthunitsinferred': 0, 'lengthtype': 'head-body length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize12Test() {
    // State
    String value = "t.l.= 2 feet 3.1 - 4.5 inches ";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': 0, 'lengthtype': 'total length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize13Test() {
    // State
    String value = "2 ft. 3.1 - 4.5 in. ";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': 0, 'lengthtype': 'total length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize14Test() {
    // State
    String value = "total length= 2 ft.";
    String expected =
        "haslength': 1, 'lengthinmm': 610, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize15Test() {
    // State
    String value = "AJR-32   186-102-23-15  15.0g";
    String expected =
        "haslength': 1, 'lengthinmm': 186, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize16Test() {
    // State
    String value = "length=8 mm";
    String expected =
        "haslength': 1, 'lengthinmm': 8, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize17Test() {
    // State
    String value = "another; length=8 mm";
    String expected =
        "haslength': 1, 'lengthinmm': 8, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize18Test() {
    // State
    String value = "another; TL_120, noise";
    String expected =
        "haslength': 1, 'lengthinmm': 120, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize19Test() {
    // State
    String value = "another; TL - 101.3mm, noise";
    String expected =
        "haslength': 1, 'lengthinmm': 101.3, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize20Test() {
    // State
    String value = "before; TL153, after";
    String expected =
        "haslength': 1, 'lengthinmm': 153, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize21Test() {
    // State
    String value = "before; Total length in catalog and specimen tag as 117, after";
    String expected =
        "haslength': 1, 'lengthinmm': 117, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize22Test() {
    // State
    String value = "before Snout vent lengths range from 16 to 23 mm. after";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'snout-vent length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize23Test() {
    // State
    String value = "Size=13 cm TL";
    String expected =
        "haslength': 1, 'lengthinmm': 130, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize24Test() {
    // State
    String value = "det_comments:31.5-58.3inTL";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'total length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize25Test() {
    // State
    String value = "SVL52mm";
    String expected =
        "haslength': 1, 'lengthinmm': 52, 'lengthunitsinferred': 0, 'lengthtype': 'snout-vent length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize26Test() {
    // State
    String value = "snout-vent length=221 mm; total length=257 mm; tail length=36 mm";
    String expected =
        "haslength': 1, 'lengthinmm': 257, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize27Test() {
    // State
    String value = "SVL 209 mm, total 272 mm, 4.4 g.";
    String expected =
        "haslength': 1, 'lengthinmm': 272, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize28Test() {
    // State
    String value = "{\"time collected\":\"0712-0900\", \"length\":\"12.0\" }";
    String expected =
        "haslength': 1, 'lengthinmm': 12.0, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize29Test() {
    // State
    String value =
        "{\"time collected\":\"1030\", \"water depth\":\"1-8\", \"bottom\":\"abrupt lava cliff dropping off to sand at 45 ft.\", \"length\":\"119-137\" }";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'total length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize30Test() {
    // State
    String value = "TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx";
    String expected =
        "haslength': 1, 'lengthinmm': 44, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize31Test() {
    // State
    String value = "{\"totalLengthInMM\":\"270-165-18-22-31\", ";
    String expected =
        "haslength': 1, 'lengthinmm': 270, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize32Test() {
    // State
    String value = "{\"length\":\"20-29\" }";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'total length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize33Test() {
    // State
    String value = "field measurements on fresh dead specimen were 157-60-20-19-21g";
    String expected =
        "haslength': 1, 'lengthinmm': 157, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize34Test() {
    // State
    String value = "f age class: adult; standard length: 63-107mm";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize35Test() {
    // State
    String value = "Rehydrated in acetic acid 7/1978-8/1987.";
    String expected =
        "haslength': 0, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize36Test() {
    // State
    String value = "age class: adult; standard length: 18.0-21.5mm";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize37Test() {
    // State
    String value = "age class: adult; standard length: 18-21.5mm";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize38Test() {
    // State
    String value = "age class: adult; standard length: 18.0-21mm";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize39Test() {
    // State
    String value = "age class: adult; standard length: 18-21mm";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize40Test() {

    String value =
        "Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,5592,5595,5594,5593,5596,5589,5587,5586,5585";
    String expected =
        "haslength': 0, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize41Test() {
    // State
    String value = "20-28mm SL";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize42Test() {
    // State
    String value = "29mm SL";
    String expected =
        "haslength': 1, 'lengthinmm': 29, 'lengthunitsinferred': 0, 'lengthtype': 'standard length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize43Test() {
    // State
    String value = "{\"measurements\":\"159-?-22-16=21.0\" }";
    String expected =
        "haslength': 1, 'lengthinmm': 159, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize44Test() {
    // State
    String value = "c701563b-dbd9-4500-184f-1ad61eb8da11";
    String expected =
        "haslength': 0, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize45Test() {
    // State
    String value = "Meas: L: 21.0";
    String expected =
        "haslength': 1, 'lengthinmm': 21.0, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize46Test() {
    // State
    String value = "Meas: L: 21.0 cm";
    String expected =
        "haslength': 1, 'lengthinmm': 210.0, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize47Test() {
    // State
    String value = "LABEL. LENGTH 375 MM.";
    String expected =
        "haslength': 1, 'lengthinmm': 375, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize48Test() {
    // State
    String value = "SL=12mm";
    String expected =
        "haslength': 1, 'lengthinmm': 12, 'lengthunitsinferred': 0, 'lengthtype': 'standard length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize49Test() {
    // State
    String value = "Size=SL 12-14 mm";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize50Test() {
    // State
    String value = "SV 1.2";
    String expected =
        "haslength': 1, 'lengthinmm': 1.2, 'lengthunitsinferred': 1, 'lengthtype': 'snout-vent length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize51Test() {
    // State
    String value = " Length: 123 mm SL";
    String expected =
        "haslength': 1, 'lengthinmm': 123, 'lengthunitsinferred': 0, 'lengthtype': 'standard length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize52Test() {
    // State
    String value = " Length: 12-34 mmSL";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'standard length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize53Test() {
    // State
    String value = "Measurements: L: 21.0 cm";
    String expected =
        "haslength': 1, 'lengthinmm': 210.0, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize54Test() {
    // State
    String value = "SVL=44";
    String expected =
        "haslength': 1, 'lengthinmm': 44, 'lengthunitsinferred': 1, 'lengthtype': 'snout-vent length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void search_and_normalize55Test() {
    // State
    String value = "SVL=0 g";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': None";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void search_and_normalize56Test() {
    // State
    String value = "SVL=44', '', 'TL=50mm";
    String expected =
        "haslength': 1, 'lengthinmm': 50, 'lengthunitsinferred': 0, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void search_and_normalize57Test() {
    // State
    String value = "TL=50', '', 'SVL=44mm";
    String expected =
        "haslength': 1, 'lengthinmm': 50, 'lengthunitsinferred': 1, 'lengthtype': 'total length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void search_and_normalize58Test() {
    // State
    String value = "TgL=50', 'some other length', 'SVL=44mm";
    String expected =
        "haslength': 1, 'lengthinmm': 44, 'lengthunitsinferred': 0, 'lengthtype': 'snout-vent length'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void search_and_normalize69Test() {
    // State
    String value = "Total Length: 185-252 mm', '', '";
    String expected =
        "haslength': 1, 'lengthinmm': None, 'lengthunitsinferred': None, 'lengthtype': 'total length range'";

    // When
    Optional<String> result = LengthParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }
}
