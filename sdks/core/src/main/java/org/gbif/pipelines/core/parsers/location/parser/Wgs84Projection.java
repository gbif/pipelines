package org.gbif.pipelines.core.parsers.location.parser;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_REPROJECTED;
import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_REPROJECTION_FAILED;
import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS;
import static org.gbif.api.vocabulary.OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84;
import static org.gbif.api.vocabulary.OccurrenceIssue.GEODETIC_DATUM_INVALID;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Set;
import java.util.TreeSet;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DatumParser;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
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

/** Utils class that reprojects to WGS84 based on geotools transformations and SRS databases. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Wgs84Projection {

  private static final DatumParser PARSER = DatumParser.getInstance();
  private static final double SUSPICIOUS_SHIFT = 0.1d;
  private static DatumAuthorityFactory datumFactory;

  static {
    try {
      datumFactory = BasicFactories.getDefault().getDatumAuthorityFactory();
    } catch (FactoryRegistryException e) {
      log.error("Failed to create geotools datum factory", e);
    }
  }

  /**
   * Reproject the given location into WGS84 location based on a known source datum or SRS. Darwin
   * Core allows not only geodetic datums but also full spatial reference systems as values for
   * "datum". The method will always return lat lons even if the processing failed. In that case
   * only issues are set and the parsing result set to fail - but with a valid payload.
   *
   * @param lat the original latitude
   * @param lon the original longitude
   * @param datum the original geodetic datum the location are in
   * @return the reprojected location or the original ones in case transformation failed
   */
  public static ParsedField<LatLng> reproject(double lat, double lon, String datum) {
    Preconditions.checkArgument(lat >= -90d && lat <= 90d);
    Preconditions.checkArgument(lon >= -180d && lon <= 180d);

    Set<String> issues = new TreeSet<>();

    if (Strings.isNullOrEmpty(datum)) {
      issues.add(GEODETIC_DATUM_ASSUMED_WGS84.name());
      return ParsedField.success(new LatLng(lat, lon), issues);
    }

    try {
      CoordinateReferenceSystem crs = parseCRS(datum);
      if (crs == null) {
        issues.add(GEODETIC_DATUM_INVALID.name());
        issues.add(GEODETIC_DATUM_ASSUMED_WGS84.name());

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
          srcPt = new double[] {lon, lat, 0};
        }

        transform.transform(srcPt, 0, dstPt, 0, 1);

        double lat2 = dstPt[1];
        double lon2 = dstPt[0];
        // verify the datum shift is reasonable
        if (Math.abs(lat - lat2) > SUSPICIOUS_SHIFT || Math.abs(lon - lon2) > SUSPICIOUS_SHIFT) {
          issues.add(COORDINATE_REPROJECTION_SUSPICIOUS.name());
          return ParsedField.fail(new LatLng(lat, lon), issues);
        }
        // flag the record if coords actually changed
        if (lat != lat2 || lon != lon2) {
          issues.add(COORDINATE_REPROJECTED.name());
        }
        return ParsedField.success(new LatLng(lat2, lon2), issues);
      }
    } catch (Exception ex) {
      log.warn(
          "Coordinate re-projection failed for datum {}, lat {} and lon {}: {}",
          datum,
          lat,
          lon,
          ex.getMessage(),
          ex);
      issues.add(COORDINATE_REPROJECTION_FAILED.name());
    }

    return ParsedField.fail(new LatLng(lat, lon), issues);
  }

  /**
   * Parses the given datum or SRS code and constructs a full 2D geographic reference system.
   *
   * @return the parsed CRS or null if it can't be interpreted
   */
  private static CoordinateReferenceSystem parseCRS(String datum) {
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
          GeodeticDatum dat = datumFactory.createGeodeticDatum(code);
          crs = new DefaultGeographicCRS(dat, DefaultEllipsoidalCS.GEODETIC_2D);

        } catch (FactoryException e1) {
          // also not a datum, no further ideas, log error
          // swallow anything and return null instead
          log.info("No CRS or DATUM for given datum code >>{}<<: {}", datum, e1.getMessage());
        }
      }
    }
    return crs;
  }
}
