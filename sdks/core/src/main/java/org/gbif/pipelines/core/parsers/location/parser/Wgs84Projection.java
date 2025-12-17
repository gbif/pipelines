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
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;

/** Utils class that reprojects to WGS84 based on geotools transformations and SRS databases. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Wgs84Projection {

  private static final double SUSPICIOUS_SHIFT = 0.1d;

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
  public static ParsedField<GeocodeRequest> reproject(double lat, double lon, String datum) {
    Preconditions.checkArgument(lat >= -90d && lat <= 90d);
    Preconditions.checkArgument(lon >= -180d && lon <= 180d);

    Set<String> issues = new TreeSet<>();

    if (Strings.isNullOrEmpty(datum)) {
      issues.add(GEODETIC_DATUM_ASSUMED_WGS84.name());
      return ParsedField.success(GeocodeRequest.create(lat, lon), issues);
    }

    try {
      CoordinateReferenceSystem crs = SpatialReferenceSystemParser.parseCRS(datum);
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

        // retain 7 digits to allow reversing the trasform,
        // see https://github.com/gbif/pipelines/issues/517 for discussion
        double lat2 = roundTo7decimals(dstPt[1]);
        double lon2 = roundTo7decimals(dstPt[0]);
        // verify the datum shift is reasonable
        if (Math.abs(lat - lat2) > SUSPICIOUS_SHIFT || Math.abs(lon - lon2) > SUSPICIOUS_SHIFT) {
          issues.add(COORDINATE_REPROJECTION_SUSPICIOUS.name());
          return ParsedField.fail(GeocodeRequest.create(lat, lon), issues);
        }
        // flag the record if coords actually changed
        if (lat != lat2 || lon != lon2) {
          issues.add(COORDINATE_REPROJECTED.name());
        }
        return ParsedField.success(GeocodeRequest.create(lat2, lon2), issues);
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

    return ParsedField.fail(GeocodeRequest.create(lat, lon), issues);
  }

  // Round to 7 decimals (~1m precision) since no way we're getting anything legitimately more
  // precise
  private static Double roundTo7decimals(Double x) {
    return x == null ? null : Math.round(x * Math.pow(10, 7)) / Math.pow(10, 7);
  }
}
