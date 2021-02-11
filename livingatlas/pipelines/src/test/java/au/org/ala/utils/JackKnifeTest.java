package au.org.ala.utils;

import static org.junit.Assert.*;

import au.org.ala.pipelines.jackknife.JackKnife;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests ported from
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/test/scala/au/org/ala/biocache/DistanceRangeParserTest.scala
 */
public class JackKnifeTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void jackKnife() {

    // too few values
    int size = 10;
    Double[] values = new Double[size];
    double[] result = JackKnife.jackknife(values, size + 1);
    assertNull(result);

    // too many outliers
    values = new Double[] {1.0, 2.0, 3.0, 4.0, 10.0, 11.0, 12.0, 13.0, 14.0, 16.0, 17.0};
    result = JackKnife.jackknife(values, values.length - 1);
    assertNull(result);

    // valid min/max
    values = new Double[] {1.0, 2.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 16.0, 17.0};
    result = JackKnife.jackknife(values, values.length - 1);
    assertNotNull(result);
    assertEquals(result[0], 8.0, 0.0);
    assertEquals(result[1], 14.0, 0.0);
  }
}
