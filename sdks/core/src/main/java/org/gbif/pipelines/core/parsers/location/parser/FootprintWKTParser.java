package org.gbif.pipelines.core.parsers.location.parser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.IdentityTransform;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

/** Parser for the Dwc Terms related to the footprintWKT. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FootprintWKTParser {

  private static CoordinateReferenceSystem WGS84;

  // WGS84 is the reference CRS.
  static {
    try {
      WGS84 = CRS.decode("EPSG:4326");
    } catch (FactoryException ex) {
      log.error("Error initiating WGS84", ex);
    }
  }

  /** Parse the DwcTerm footprintWKT using a CRS/footprintSRS as reference. */
  public static ParsedField<String> parseFootprintWKT(
      CoordinateReferenceSystem footprintSRS, String footprintWKT) {
    try {
      MathTransform transform;
      if (footprintSRS == null) {
        transform = IdentityTransform.create(WGS84.getCoordinateSystem().getDimension());
      } else {
        transform = CRS.findMathTransform(footprintSRS, WGS84, true);
      }

      WKTReader wktReader = new WKTReader();
      Geometry geometry = wktReader.read(footprintWKT);
      return ParsedField.<String>builder()
          .result(JTS.transform(geometry, transform).toText())
          .successful(true)
          .build();
    } catch (Exception ex) {
      return ParsedField.fail(OccurrenceIssue.FOOTPRINT_WKT_INVALID.name());
    }
  }
}
