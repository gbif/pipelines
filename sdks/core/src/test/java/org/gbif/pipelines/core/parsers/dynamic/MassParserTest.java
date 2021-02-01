package org.gbif.pipelines.core.parsers.dynamic;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class MassParserTest {

  @Test
  public void parser1Test() {
    // State
    String value = "762-292-121-76 2435.0g";
    String expected = "'key': '_shorthand_', 'value': '2435.0', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser2Test() {
    // State
    String value = "TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx";
    String expected = "'key': 'Weight', 'value': '0.77', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser3Test() {
    // State
    String value = "Note in catalog: Mus. SW Biol. NK 30009; 91-0-17-22-62g";
    String expected = "'key': '_shorthand_', 'value': '62', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser4Test() {
    // State
    String value = "body mass=20 g";
    String expected = "'key': 'body mass', 'value': '20', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser5Test() {
    // State
    String value = "2 lbs. 3.1 - 4.5 oz ";
    String expected = "'key': '_english_', 'value': ['2', '3.1 - 4.5'], 'units': ['lbs.', 'oz']";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser6Test() {
    // State
    String value =
        "{\"totalLengthInMM\":\"x\", \"earLengthInMM\":\"20\", \"weight\":\"[139.5] g\"}";
    String expected = "'key': 'weight', 'value': '[139.5]', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser7Test() {
    // State
    String value =
        "{\"fat\":\"No fat\", \"gonads\":\"Testes 10 x 6 mm.\", \"molt\":\"No molt\", \"stomach contents\":\"Not recorded\", \"weight\":\"94 gr.\"";
    String expected = "'key': 'weight', 'value': '94', 'units': 'gr.'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser8Test() {
    // State
    String value = "Note in catalog: 83-0-17-23-fa64-35g";
    String expected = "'key': '_shorthand_', 'value': '35', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser9Test() {
    // State
    String value = "{\"measurements\":\"20.2g, SVL 89.13mm\" }";
    String expected = "'key': 'measurements', 'value': '20.2', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser10Test() {
    // State
    String value = "Body: 15 g";
    String expected = "'key': 'Body', 'value': '15', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser11Test() {
    // State
    String value = "82-00-15-21-tr7-fa63-41g";
    String expected = "'key': '_shorthand_', 'value': '41', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser12Test() {
    // State
    String value = "weight=5.4 g; unformatted measurements=77-30-7-12=5.4";
    String expected = "'key': 'weight', 'value': '5.4', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser13Test() {
    // State
    String value = "unformatted measurements=77-30-7-12=5.4; weight=5.4;";
    String expected = "'key': 'measurements', 'value': '5.4', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser14Test() {
    // State
    String value = "{\"totalLengthInMM\":\"270-165-18-22-31\", ";
    String expected = "'key': '_shorthand_', 'value': '31', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser15Test() {
    // State
    String value = "{\"measurements\":\"143-63-20-17=13 g\" }";
    String expected = "'key': 'measurements', 'value': '13', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser16Test() {
    // State
    String value = "143-63-20-17=13";
    String expected = "'key': '_shorthand_', 'value': '13', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser17Test() {
    // State
    String value =
        "reproductive data: Testes descended -10x7 mm; sex: male; unformatted measurements: 181-75-21-18=22 g";
    String expected = "'key': 'measurements', 'value': '22', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser18Test() {
    // State
    String value = "{ \"massingrams\"=\"20.1\" }";
    String expected = "'key': 'massingrams', 'value': '20.1', 'units': 'grams'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser19Test() {
    // State
    String value =
        " {\"gonadLengthInMM_1\":\"10\", \"gonadLengthInMM_2\":\"6\", \"weight\":\"1,192.0\" }";
    String expected = "'key': 'weight', 'value': '1,192.0', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser20Test() {
    // State
    String value = "\"weight: 20.5-31.8";
    String expected = "'key': 'weight', 'value': '20.5-31.8', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser21Test() {
    // State
    String value = "\"weight: 20.5-32";
    String expected = "'key': 'weight', 'value': '20.5-32', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser22Test() {
    // State
    String value = "\"weight: 21-31.8";
    String expected = "'key': 'weight', 'value': '21-31.8', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser23Test() {
    // State
    String value = "\"weight: 21-32";
    String expected = "'key': 'weight', 'value': '21-32', 'units': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser24Test() {
    // State
    String value =
        "Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,5592,5595,5594,5593,5596,5589,5587,5586,5585";
    String expected = null;

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser25Test() {
    // State
    String value = "weight=5.4 g; unformatted measurements=77-x-7-12=5.4";
    String expected = "'key': 'weight', 'value': '5.4', 'units': 'g'";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void parser26Test() {
    // State
    String value = "c701563b-dbd9-4500-184f-1ad61eb8da11";
    String expected = null;

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize1Test() {
    // State
    String value = "762-292-121-76 2435.0g";
    String expected = "{'hasmass': 1, 'massing': 2435.0, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize2Test() {
    // State
    String value = "TL (mm) 44,SL (mm) 38,Weight (g) 0.77 xx";
    String expected = "{'hasmass': 1, 'massing': 0.77, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize3Test() {
    // State
    String value = "Note in catalog: Mus. SW Biol. NK 30009; 91-0-17-22-62g";
    String expected = "{'hasmass': 1, 'massing': 62, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize4Test() {
    // State
    String value = "body mass=20 g";
    String expected = "{'hasmass': 1, 'massing': 20, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize5Test() {
    // State
    String value = "2 lbs. 3.1 - 4.5 oz ";
    String expected = "{'hasmass': 1, 'massing': None, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize6Test() {
    // State
    String value =
        "{\"totalLengthInMM\":\"x\", \"earLengthInMM\":\"20\", \"weight\":\"[139.5] g\" }";
    String expected = "{'hasmass': 1, 'massing': 139.5, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize7Test() {
    // State
    String value =
        "{\"fat\":\"No fat\", \"gonads\":\"Testes 10 x 6 mm.\", \"molt\":\"No molt\",\"stomach contents\":\"Not recorded\", \"weight\":\"94 gr.\"";
    String expected = "{'hasmass': 1, 'massing': 94, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize8Test() {
    // State
    String value = "Note in catalog: 83-0-17-23-fa64-35g";
    String expected = "{'hasmass': 1, 'massing': 35, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize9Test() {
    // State
    String value = "{\"measurements\":\"20.2g, SVL 89.13mm\" }";
    String expected = "{'hasmass': 1, 'massing': 20.2, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize10Test() {
    // State
    String value = "Body: 15 g";
    String expected = "{'hasmass': 1, 'massing': 15, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize11Test() {
    // State
    String value = "82-00-15-21-tr7-fa63-41g";
    String expected = "{'hasmass': 1, 'massing': 41, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize12Test() {
    // State
    String value = "weight=5.4 g; unformatted measurements=77-30-7-12=5.4";
    String expected = "{'hasmass': 1, 'massing': 5.4, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize13Test() {
    // State
    String value = "unformatted measurements=77-30-7-12=5.4; weight=5.4;";
    String expected = "{'hasmass': 1, 'massing': 5.4, 'massunitsinferred': 1";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize14Test() {
    // State
    String value = "{\"totalLengthInMM\":\"270-165-18-22-31\", ";
    String expected = "{'hasmass': 1, 'massing': 31, 'massunitsinferred': 1";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize15Test() {
    // State
    String value = "{\"measurements\":\"143-63-20-17=13 g\" }";
    String expected = "{'hasmass': 1, 'massing': 13, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize16Test() {
    // State
    String value = "143-63-20-17=13";
    String expected = "{'hasmass': 1, 'massing': 13, 'massunitsinferred': 1";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize17Test() {
    // State
    String value =
        "reproductive data: Testes descended -10x7 mm; sex: male; unformatted measurements: 181-75-21-18=22 g";
    String expected = "{'hasmass': 1, 'massing': 22, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize18Test() {
    // State
    String value = "{ \"massingrams\"=\"20.1\" }";
    String expected = "{'hasmass': 1, 'massing': 20.1, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize19Test() {
    // State
    String value =
        " {\"gonadLengthInMM_1\":\"10\", \"gonadLengthInMM_2\":\"6\", \"weight\":\"1,192.0\" }";
    String expected = "{'hasmass': 1, 'massing': 1192.0, 'massunitsinferred': 1";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize20Test() {
    // State
    String value = "\"weight: 20.5-31.8";
    String expected = "{'hasmass': 1, 'massing': None, 'massunitsinferred': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize21Test() {
    // State
    String value = "\"weight: 20.5-32";
    String expected = "{'hasmass': 1, 'massing': None, 'massunitsinferred': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize22Test() {
    // State
    String value = "\"weight: 21-31.8";
    String expected = "{'hasmass': 1, 'massing': None, 'massunitsinferred': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize23Test() {
    // State
    String value = "\"weight: 21-32";
    String expected = "{'hasmass': 1, 'massing': None, 'massunitsinferred': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize24Test() {
    // State
    String value =
        "Specimen #'s - 5491,5492,5498,5499,5505,5526,5527,5528,5500,5507,5508,5590,5592,5595,5594,5593,5596,5589,5587,5586,5585";
    String expected = "{'hasmass': 0, 'massing': None, 'massunitsinferred': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize25Test() {
    // State
    String value = "weight=5.4 g; unformatted measurements=77-x-7-12=5.4";
    String expected = "{'hasmass': 1, 'massing': 5.4, 'massunitsinferred': 0";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize26Test() {
    // State
    String value = "c701563b-dbd9-4500-184f-1ad61eb8da11";
    String expected = "{'hasmass': 0, 'massing': None, 'massunitsinferred': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }

  @Test
  public void searchAndNormalize27Test() {
    // State
    String value = "body mass=0 g";
    String expected = "{'hasmass': 1, 'massing': None, 'massunitsinferred': None";

    // When
    Optional<String> result = MassParser.parse(value);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(expected, result.get());
  }
}
