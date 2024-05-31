package uk.org.nbn.util

import org.apache.commons.math3.util.Precision
import org.geotools.geometry.GeneralDirectPosition
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory

/**
  * GIS Utilities.
  */
object GISUtil {

  val WGS84_EPSG_Code = "EPSG:4326"

  /**
    * Re-projects coordinates into WGS 84
    *
    * @param coordinate1 first coordinate. If source value is easting/northing, then this should be the easting value.
    *                    Otherwise it should be the latitude
    * @param coordinate2 first coordinate. If source value is easting/northing, then this should be the northing value.
    *                    Otherwise it should be the longitude
    * @param sourceCrsEpsgCode epsg code for the source CRS, e.g. EPSG:4202 for AGD66
    * @param decimalPlacesToRoundTo number of decimal places to round the reprojected coordinates to
    * @return Reprojected coordinates (latitude, longitude), or None if the operation failed.
    */
  def reprojectCoordinatesToWGS84(coordinate1: Double, coordinate2: Double, sourceCrsEpsgCode: String,
                                  decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    try {
      val wgs84CRS = DefaultGeographicCRS.WGS84
      val sourceCRS = CRS.decode(sourceCrsEpsgCode)
      val transformOp = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, wgs84CRS)
      val directPosition = new GeneralDirectPosition(coordinate1, coordinate2)
      val wgs84LatLong = transformOp.getMathTransform().transform(directPosition, null)

      //NOTE - returned coordinates are longitude, latitude, despite the fact that if
      //converting latitude and longitude values, they must be supplied as latitude, longitude.
      //No idea why this is the case.
      val longitude = wgs84LatLong.getOrdinate(0)
      val latitude = wgs84LatLong.getOrdinate(1)

      val roundedLongitude = Precision.round(longitude, decimalPlacesToRoundTo)
      val roundedLatitude = Precision.round(latitude, decimalPlacesToRoundTo)

      Some(roundedLatitude.toString, roundedLongitude.toString)
    } catch {
      case ex: Exception => None
    }
  }
}
