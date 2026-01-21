package org.gbif.pipelines.core.parsers.location;

import java.util.stream.Stream;
import org.gbif.pipelines.core.parsers.location.parser.SpatialReferenceSystemParser;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.NamedIdentifier;
import org.junit.Assert;
import org.junit.Test;

/** Tests for Spatial Reference Systems parsing. */
public class SpatialReferenceSystemParserTest {

  private static final Stream<String> VALID_SRSs =
      Stream.of("EPSG:32629", "EPSG:27700", "EPSG:32636", "EPSG:4326", "EPSG:28992");

  /** Test that a list CRS reported to GBIF through occurrence records. */
  @Test
  public void testValidSRSs() {
    VALID_SRSs.forEach(
        crs -> {
          CoordinateReferenceSystem coordinateReferenceSystem =
              SpatialReferenceSystemParser.parseCRS(crs);

          Assert.assertNotNull(coordinateReferenceSystem);

          // Is the source identifier in the list of identifiers.
          Assert.assertTrue(
              coordinateReferenceSystem.getIdentifiers().stream()
                  .anyMatch(
                      referenceIdentifier -> {
                        NamedIdentifier namedIdentifier = (NamedIdentifier) referenceIdentifier;
                        return namedIdentifier.toInternationalString().toString().equals(crs);
                      }));
        });
  }

  @Test
  public void testInvalidSRS() {
    CoordinateReferenceSystem coordinateReferenceSystem =
        SpatialReferenceSystemParser.parseCRS("EPSG:326399");
    Assert.assertNull(coordinateReferenceSystem);
  }
}
