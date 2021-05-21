package au.org.ala.parser;

import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.parser.DistanceParser;
import java.util.UnknownFormatConversionException;
import org.junit.Test;

/**
 * Tests ported from
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/test/scala/au/org/ala/biocache/DistanceRangeParserTest.scala
 */
public class UncertaintyRangeParserTest {

  @Test
  public void parseRange() {
    double value = DistanceParser.parse("2000");
    assertEquals(2000f, value, 0);

    value = DistanceParser.parse("2000.01[],");
    assertEquals(Double.valueOf("2000.01"), Double.valueOf(value));

    value = DistanceParser.parse("100metres,");
    assertEquals(Double.valueOf("100"), Double.valueOf(value));

    value = DistanceParser.parse("100ft,");
    assertEquals(30.48f, value, 0.001);

    value = DistanceParser.parse("100km,");
    assertEquals(100000f, value, 0);

    value = DistanceParser.parse("1000inches,");
    assertEquals(25.4, value, 0);

    value = DistanceParser.parse(" 1m-20km");
    assertEquals(20000f, value, 0);

    value = DistanceParser.parse(" 1feet-20kilometres");
    assertEquals(20000f, value, 0);

    // GBIF parser's precision set to
    value = DistanceParser.parse(" 1km-20feet,");
    assertEquals(6.1d, value, 0.01);

    value = DistanceParser.parse(" >15,");
    assertEquals(15f, value, 0);

    value = DistanceParser.parse(" >15kilometers,");
    assertEquals(15000f, value, 0);
  }

  @Test(expected = UnknownFormatConversionException.class)
  public void invalidUncertainty() {
    DistanceParser.parse("test");
  }
}
