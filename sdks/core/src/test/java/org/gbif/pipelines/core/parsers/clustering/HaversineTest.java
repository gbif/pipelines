package org.gbif.pipelines.core.parsers.clustering;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class HaversineTest {

  @Test
  public void distanceTest() {
    double distance = Haversine.distance(21.8656, -102.909, 21.86558d, -102.90929d);
    assertTrue("Distance exceeds 200m", distance < 0.2);
  }

  @Test
  public void distance1Test() {
    double distance = Haversine.distance(21.506, -103.092, 21.50599d, -103.09193);
    assertTrue("Distance exceeds 200m", distance < 0.2);
  }
}
