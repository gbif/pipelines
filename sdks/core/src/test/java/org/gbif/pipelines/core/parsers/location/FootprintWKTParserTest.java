package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.FootprintWKTParser;
import org.gbif.pipelines.core.parsers.location.parser.SpatialReferenceSystemParser;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/** Tests of footprintWKT parser. */
public class FootprintWKTParserTest {

  @Test
  public void validFootprintWKTTest() {
    CoordinateReferenceSystem fromCRS = SpatialReferenceSystemParser.parseCRS("EPSG:28992");
    ParsedField<String> footPrintWKT =
        FootprintWKTParser.parseFootprintWKT(
            fromCRS,
            "POLYGON((100000 515000,100000 520000,105000 520000,105000 515000,100000 515000))");
    Assert.assertTrue(footPrintWKT.isSuccessful());
  }

  @Test
  public void invalidFootPrintWKTTest() {
    CoordinateReferenceSystem fromCRS = SpatialReferenceSystemParser.parseCRS("EPSG:28992");
    ParsedField<String> footPrintWKT =
        FootprintWKTParser.parseFootprintWKT(fromCRS, "POLYGON((0 0, 0 10, 10 10, 10 0))");
    Assert.assertFalse(footPrintWKT.isSuccessful());
    footPrintWKT.getIssues().contains(OccurrenceIssue.FOOTPRINT_WKT_INVALID);
  }
}
