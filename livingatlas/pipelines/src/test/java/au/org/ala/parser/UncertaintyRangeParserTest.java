package au.org.ala.parser;

import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.parser.UncertaintyParser;
import java.util.UnknownFormatConversionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests ported from
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/test/scala/au/org/ala/biocache/DistanceRangeParserTest.scala
 */
public class UncertaintyRangeParserTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void parseRange() {
    double value = UncertaintyParser.parse("2000");
    assertEquals(2000f, value, 0);

    value = UncertaintyParser.parse("2000.01[],");
    assertEquals(Double.valueOf("2000.01"), Double.valueOf(value));

    value = UncertaintyParser.parse("100metres,");
    assertEquals(Double.valueOf("100"), Double.valueOf(value));

    value = UncertaintyParser.parse("100ft,");
    assertEquals(30.48f, value, 0.001);

    value = UncertaintyParser.parse("100km,");
    assertEquals(100000f, value, 0);

    value = UncertaintyParser.parse("1000inches,");
    assertEquals(25.4, value, 0);

    value = UncertaintyParser.parse(" 1m-20km");
    assertEquals(20000f, value, 0);

    value = UncertaintyParser.parse(" 1feet-20kilometres");
    assertEquals(20000f, value, 0);

    // GBIF parser's precision set to
    value = UncertaintyParser.parse(" 1km-20feet,");
    assertEquals(6.1d, value, 0.01);

    value = UncertaintyParser.parse(" >15,");
    assertEquals(15f, value, 0);

    value = UncertaintyParser.parse(" >15kilometers,");
    assertEquals(15000f, value, 0);
  }

  @Test(expected = UnknownFormatConversionException.class)
  public void invalidUncertainty() {
    UncertaintyParser.parse("test");
  }
}
