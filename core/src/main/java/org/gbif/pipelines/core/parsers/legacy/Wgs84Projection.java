package org.gbif.pipelines.core.parsers.legacy;

import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DatumParser;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.InterpretationIssue;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.io.avro.IssueType;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.geotools.factory.BasicFactories;
import org.geotools.factory.FactoryRegistryException;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.cs.DefaultEllipsoidalCS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.DatumAuthorityFactory;
import org.opengis.referencing.datum.GeodeticDatum;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils class that reprojects to WGS84 based on geotools transformations and SRS databases.
 */
public class Wgs84Projection {

  private static final Logger LOG = LoggerFactory.getLogger(Wgs84Projection.class);
  private static final DatumParser PARSER = DatumParser.getInstance();
  private static final double SUSPICIOUS_SHIFT = 0.1d;
  private static DatumAuthorityFactory DATUM_FACTORY;

  private Wgs84Projection() {}

  static {
    try {
      DATUM_FACTORY = BasicFactories.getDefault().getDatumAuthorityFactory();
      LOG.debug("Wgs84Projection utils created");
    } catch (FactoryRegistryException e) {
      LOG.error("Failed to create geotools datum factory", e);
    }
  }

  /**
   * Reproject the given location into WGS84 location based on a known source datum or SRS.
   * Darwin Core allows not only geodetic datums but also full spatial reference systems as values for "datum".
   * The method will always return lat lons even if the processing failed. In that case only issues are set and the
   * parsing result set to fail - but with a valid payload.
   *
   * @param lat   the original latitude
   * @param lon   the original longitude
   * @param datum the original geodetic datum the location are in
   *
   * @return the reprojected location or the original ones in case transformation failed
   */
  public static ParsedField<LatLng> reproject(double lat, double lon, String datum) {
    Preconditions.checkArgument(lat >= -90d && lat <= 90d);
    Preconditions.checkArgument(lon >= -180d && lon <= 180d);

    List<InterpretationIssue> issues = new ArrayList<>();

    if (Strings.isNullOrEmpty(datum)) {
      issues.add(InterpretationIssue.newIssue(IssueType.GEODETIC_DATUM_ASSUMED_WGS84, DwcTerm.geodeticDatum));
      return ParsedField.success(new LatLng(lat, lon), issues);
    }

    try {
      CoordinateReferenceSystem crs = parseCRS(datum);
      if (crs == null) {
        issues.add(InterpretationIssue.newIssue(IssueType.GEODETIC_DATUM_INVALID, DwcTerm.geodeticDatum));
        issues.add(InterpretationIssue.newIssue(IssueType.GEODETIC_DATUM_ASSUMED_WGS84, DwcTerm.geodeticDatum));

      } else {
        MathTransform transform = CRS.findMathTransform(crs, DefaultGeographicCRS.WGS84, true);
        // different CRS may swap the x/y axis for lat lon, so check first:
        double[] srcPt;
        double[] dstPt = new double[3];
        if (CRS.getAxisOrder(crs) == CRS.AxisOrder.NORTH_EAST) {
          // lat lon
          srcPt = new double[] {lat, lon, 0};
        } else {
          // lon lat
          LOG.debug("Use lon/lat ordering for reprojection with datum={} and lat/lon={}/{}", datum, lat, lon);
          srcPt = new double[] {lon, lat, 0};
        }

        transform.transform(srcPt, 0, dstPt, 0, 1);

        double lat2 = dstPt[1];
        double lon2 = dstPt[0];
        // verify the datum shift is reasonable
        if (Math.abs(lat - lat2) > SUSPICIOUS_SHIFT || Math.abs(lon - lon2) > SUSPICIOUS_SHIFT) {
          issues.add(InterpretationIssue.newIssue(IssueType.COORDINATE_REPROJECTION_SUSPICIOUS, DwcTerm.geodeticDatum));
          LOG.debug("Found suspicious shift for datum={} and lat/lon={}/{} so returning failure and keeping orig coord",
            datum, lat, lon);
          return ParsedField.fail(new LatLng(lat, lon), issues);
        }
        // flag the record if coords actually changed
        if (Double.compare(lat, lat2) + Double.compare(lon, lon2) != 0 ) {
          issues.add(InterpretationIssue.newIssue(IssueType.COORDINATE_REPROJECTED, DwcTerm.geodeticDatum));
        }
        return ParsedField.success(new LatLng(lat2, lon2), issues);
      }
    } catch (Exception e) {
      issues.add(InterpretationIssue.newIssue(IssueType.COORDINATE_REPROJECTION_FAILED, DwcTerm.geodeticDatum));
      LOG.debug("Coordinate reprojection failed with datum={} and lat/lon={}/{}: {}", datum, lat, lon, e.getMessage());
    }

    return ParsedField.fail(new LatLng(lat, lon), issues);
  }

  /**
   * Parses the given datum or SRS code and constructs a full 2D geographic reference system.
   *
   * @return the parsed CRS or null if it can't be interpreted
   */
  @VisibleForTesting
  protected static CoordinateReferenceSystem parseCRS(String datum) {
    CoordinateReferenceSystem crs = null;
    ParseResult<Integer> epsgCode = PARSER.parse(datum);
    if (epsgCode.isSuccessful()) {
      final String code = "EPSG:" + epsgCode.getPayload();

      // first try to create a full fledged CRS from the given code
      try {
        crs = CRS.decode(code);

      } catch (FactoryException e) {
        // that didn't work, maybe it is *just* a datum
        try {
          GeodeticDatum dat = DATUM_FACTORY.createGeodeticDatum(code);
          crs = new DefaultGeographicCRS(dat, DefaultEllipsoidalCS.GEODETIC_2D);

        } catch (FactoryException e1) {
          // also not a datum, no further ideas, log error
          // swallow anything and return null instead
          LOG.info("No CRS or DATUM for given datum code >>{}<<: {}", datum, e1.getMessage());
        }
      }
    }
    return crs;
  }
}
