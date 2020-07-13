package au.org.ala.parser;

import static org.junit.Assert.assertEquals;

import au.org.ala.pipelines.parser.DistanceRangeParser;
import com.mchange.util.AssertException;
import java.util.UnknownFormatConversionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests ported from
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/test/scala/au/org/ala/biocache/DistanceRangeParserTest.scala
 */
public class DistanceRangeParserTest {
  @Test
  public void parseRange(){
    double value = DistanceRangeParser.parse("2000");
    assertEquals(2000f, value, 0);

    value = DistanceRangeParser.parse("2000.01[],");
    assertEquals(Double.valueOf("2000.01"),Double.valueOf(value));

    value = DistanceRangeParser.parse("100m,");
    assertEquals(Double.valueOf("100"),Double.valueOf(value));

    value = DistanceRangeParser.parse("100ft,");
    assertEquals(30.48f,value,0.001);

    value = DistanceRangeParser.parse("100km,");
    assertEquals(100000f,value,0);

    value = DistanceRangeParser.parse(" 1km-20km,");
    assertEquals(20000f,value,0);

    value = DistanceRangeParser.parse(" 1km-20ft,");
    assertEquals(6.096f,value,0.003);

    value = DistanceRangeParser.parse(" >15,");
    assertEquals(15f,value,0);

    value = DistanceRangeParser.parse(" >15km,");
    assertEquals(15000f,value,0);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test(expected = UnknownFormatConversionException.class)
  public void invalidUncertainty(){
    DistanceRangeParser.parse("test");
  }
}
