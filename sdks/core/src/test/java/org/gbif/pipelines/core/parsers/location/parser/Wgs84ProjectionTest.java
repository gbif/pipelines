package org.gbif.pipelines.core.parsers.location.parser;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.junit.Test;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_REPROJECTED;
import static org.junit.Assert.*;

public class Wgs84ProjectionTest {

  @Test
  public void testReprojection() {
    // State
    double lat = 18.826233;
    double lng = -68.600660;
    String datum = "NAD 27";

    // When
    ParsedField<LatLng> result = Wgs84Projection.reproject(lat, lng, datum);

    // Should
    assertTrue(result.isSuccessful());
    assertExpected(result, LatLng.create(18.826932, -68.6000422), COORDINATE_REPROJECTED);
  }

  private void assertExpected(ParsedField<LatLng> pr, Object expected, OccurrenceIssue... issue) {
    assertNotNull(pr);
    assertTrue(pr.isSuccessful());
    assertNotNull(pr.getResult());
    assertEquals(expected, pr.getResult());
    if (issue == null) {
      assertTrue(pr.getIssues().isEmpty());
    } else {
      assertEquals(issue.length, pr.getIssues().size());
      for (OccurrenceIssue iss : issue) {
        assertTrue(pr.getIssues().contains(iss.name()));
      }
    }
  }
}
