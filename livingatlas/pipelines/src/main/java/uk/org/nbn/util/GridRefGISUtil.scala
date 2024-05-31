package uk.org.nbn.util

import org.apache.commons.math3.util.Precision
import org.geotools.geometry.GeneralDirectPosition
import org.geotools.referencing.CRS
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory
import org.slf4j.LoggerFactory

/**
  * GIS Utilities.
  */
object GridRefGISUtil {

  val WGS84_EPSG_Code = "EPSG:4326"

  protected val logger = LoggerFactory.getLogger("Config")

  //returns northing and eastings, not lat-lon
  def reprojectCoordinatesWGS84ToOSGB36(coordinate1: Double, coordinate2: Double,
                                        decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    try {
      val wgs84CRS = CRS.decode("EPSG:4326", false)
      val osgb36CRS = CRS.decode("EPSG:27700", false)

      val transformOp = new DefaultCoordinateOperationFactory().createOperation(wgs84CRS, osgb36CRS)
      val directPosition = new GeneralDirectPosition(coordinate1, coordinate2)
      val osgb36LatLong = transformOp.getMathTransform().transform(directPosition, null)

      val longitude = osgb36LatLong.getOrdinate(0)
      val latitude = osgb36LatLong.getOrdinate(1)

      val roundedLongitude = Precision.round(longitude, decimalPlacesToRoundTo)
      val roundedLatitude = Precision.round(latitude, decimalPlacesToRoundTo)

      Some(roundedLatitude.toString, roundedLongitude.toString)
    } catch {
      case ex: Exception => {
        logger.error("failed reprojectCoordinatesWGS84ToOSGB36:",ex)
        None
      }
    }
  }

  //returns northing and eastings, not lat-lon
  def reprojectCoordinatesWGS84ToOSNI(coordinate1: Double, coordinate2: Double,
                                        decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    try {
      val wgs84CRS = CRS.decode("EPSG:4326", false)
      val osniCRS = CRS.decode("EPSG:29903", false) //https://epsg.io/29903

      val transformOp = new DefaultCoordinateOperationFactory().createOperation(wgs84CRS, osniCRS)
      val directPosition = new GeneralDirectPosition(coordinate1, coordinate2)
      val osniLatLong = transformOp.getMathTransform().transform(directPosition, null)

      //getting discrepancies in 1m and 10m grid sometimes. though that could be an issue on the other side as opposed to with this code
      //if set northings and eastings to match then get same grid
      val longitude = osniLatLong.getOrdinate(0)
      val latitude = osniLatLong.getOrdinate(1)

      val roundedLongitude = Precision.round(longitude, decimalPlacesToRoundTo)
      val roundedLatitude = Precision.round(latitude, decimalPlacesToRoundTo)

      Some(roundedLatitude.toString, roundedLongitude.toString)
    } catch {
      case ex: Exception => None
    }
  }

  /**
    * This is a port of this javascript code:
   *
  * http://www.movable-type.co.uk/scripts/latlong-gridref.html (i.e. https://cdn.rawgit.com/chrisveness/geodesy/v1.1.3/osgridref.js)
  */

  def coordinatesOSGB36toNorthingEasting(lat: Double, lon: Double, decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    //TODO: it would be good if this came from a library instead of written the hard (and potentially buggy) way.
    try {
      val φ = lat.toRadians
      val λ = lon.toRadians

      val a = 6377563.396
      val b = 6356256.909 // Airy 1830 major & minor semi-axes
      val F0 = 0.9996012717 // NatGrid scale factor on central meridian
      val φ0 = (49).toRadians
      val λ0 = (-2).toRadians // NatGrid true origin is 49°N,2°W
      val N0 = -100000
      val E0 = 400000 // northing & easting of true origin, metres
      val e2 = 1 - (b * b) / (a * a) // eccentricity squared
      val n = (a - b) / (a + b)
      val n2 = n * n
      val n3 = n * n * n; // n, n², n³

      val cosφ = Math.cos(φ)
      val sinφ = Math.sin(φ)
      val ν = a * F0 / Math.sqrt(1 - e2 * sinφ * sinφ) // nu = transverse radius of curvature
      val ρ = a * F0 * (1 - e2) / Math.pow(1 - e2 * sinφ * sinφ, 1.5) // rho = meridional radius of curvature
      val η2 = ν / ρ - 1 // eta = ?

      val Ma = (1 + n + (5.0 / 4.0) * n2 + (5.0 / 4.0) * n3) * (φ - φ0)
      val Mb = (3 * n + 3 * n * n + (21.0 / 8.0) * n3) * Math.sin(φ - φ0) * Math.cos(φ + φ0)
      val Mc = ((15.0 / 8.0) * n2 + (15.0 / 8.0) * n3) * Math.sin(2 * (φ - φ0)) * Math.cos(2 * (φ + φ0))
      val Md = (35.0 / 24.0) * n3 * Math.sin(3 * (φ - φ0)) * Math.cos(3 * (φ + φ0))
      val M = b * F0 * (Ma - Mb + Mc - Md) // meridional arc

      val cos3φ = cosφ * cosφ * cosφ
      val cos5φ = cos3φ * cosφ * cosφ
      val tan2φ = Math.tan(φ) * Math.tan(φ)
      val tan4φ = tan2φ * tan2φ

      val I = M + N0
      val II = (ν / 2) * sinφ * cosφ
      val III = (ν / 24) * sinφ * cos3φ * (5 - tan2φ + 9 * η2)
      val IIIA = (ν / 720) * sinφ * cos5φ * (61 - 58 * tan2φ + tan4φ)
      val IV = ν * cosφ
      val V = (ν / 6) * cos3φ * (ν / ρ - tan2φ)
      val VI = (ν / 120) * cos5φ * (5 - 18 * tan2φ + tan4φ + 14 * η2 - 58 * tan2φ * η2)

      val Δλ = λ - λ0
      val Δλ2 = Δλ * Δλ
      val Δλ3 = Δλ2 * Δλ
      val Δλ4 = Δλ3 * Δλ
      val Δλ5 = Δλ4 * Δλ
      val Δλ6 = Δλ5 * Δλ

      var N = I + II * Δλ2 + III * Δλ4 + IIIA * Δλ6
      var E = E0 + IV * Δλ + V * Δλ3 + VI * Δλ5

      N = Precision.round(N, decimalPlacesToRoundTo) //(3 decimals = mm precision)
      E = Precision.round(E, decimalPlacesToRoundTo)
      Some(N.toString, E.toString)
    } catch {
      case ex: Exception => None
    }
  }

  //port of http://www.carabus.co.uk/ll_ngr.html
  //not used at present
  //lat lon would first need to be Helmert transformed WGS84 -> Irish ellipsoid
  def coordinatesLatLonToNorthingEasting(lat: Double, lon: Double, gridType: String): Option[(String, String)] = {
    try {
      var phi = lat.toRadians // convert latitude to radians
      var lam = lon.toRadians // convert longitude to radians
      //assume OSGB values
      var a = 6377563.396                           // OSGB semi-major axis
      var b = 6356256.91                            // OSGB semi-minor axis
      var e0 = 400000                               // OSGB easting of false origin
      var n0 = -100000                              // OSGB northing of false origin
      var f0 = 0.9996012717                         // OSGB scale factor on central meridian
      var e2 = 0.0066705397616                      // OSGB eccentricity squared
      var lam0 = -0.034906585039886591              // OSGB false east
      var phi0 = 0.85521133347722145                // OSGB false north
      if (gridType == "Irish") {
        a = 6377340.189                             // OSI semi-major
        b = 6356034.447                             // OSI semi-minor
        e0 = 200000                                 // OSI easting of false origin
        n0 = 250000                                 // OSI northing of false origin
        f0 = 1.000035                               // OSI scale factor on central meridian
        e2 = 0.00667054015                          // OSI eccentricity squared
        lam0 = -0.13962634015954636615389526147909  // OSI false east
        phi0 = 0.93375114981696632365417456114141   // OSI false north
      }
      val af0 = a * f0
      val bf0 = b * f0

      // easting
      val slat2 = Math.sin(phi) * Math.sin(phi)
      val nu = af0 / (Math.sqrt(1 - (e2 * slat2)))
      val rho = (nu * (1 - e2)) / (1 - (e2 * slat2))
      val eta2 = (nu / rho) - 1
      val p = lam - lam0
      val IV = nu * Math.cos(phi)
      val clat3 = Math.pow(Math.cos(phi), 3)
      val tlat2 = Math.tan(phi) * Math.tan(phi)
      val V = (nu / 6) * clat3 * ((nu / rho) - tlat2)
      val clat5 = Math.pow(Math.cos(phi), 5)
      val tlat4 = Math.pow(Math.tan(phi), 4)
      val VI = (nu / 120) * clat5 * ((5 - (18 * tlat2)) + tlat4 + (14 * eta2) - (58 * tlat2 * eta2))
      var east = e0 + (p * IV) + (Math.pow(p, 3) * V) + (Math.pow(p, 5) * VI)

      // northing
      val n = (af0 - bf0) / (af0 + bf0)
      val M = Marc(bf0, n, phi0, phi)
      val I = M + n0
      val II = (nu / 2) * Math.sin(phi) * Math.cos(phi)
      val III = ((nu / 24) * Math.sin(phi) * Math.pow(Math.cos(phi), 3)) * (5 - Math.pow(Math.tan(phi), 2) + (9 * eta2))
      val IIIA = ((nu / 720) * Math.sin(phi) * clat5) * (61 - (58 * tlat2) + tlat4)
      var north = I + ((p * p) * II) + (Math.pow(p, 4) * III) + (Math.pow(p, 6) * IIIA)

      east = Math.round(east); // round to whole number
      north = Math.round(north); // round to whole number

      Some(north.toString, east.toString)
    } catch {
      case ex: Exception => None
    }
  }

  def Marc(bf0: Double, n: Double, phi0: Double, phi: Double): Double = {
    val MarcVal = bf0 * (((1 + n + ((5.0 / 4.0) * (n * n)) + ((5.0 / 4.0) * (n * n * n))) * (phi - phi0))
      - (((3 * n) + (3 * (n * n)) + ((21.0 / 8.0) * (n * n * n))) * (Math.sin(phi - phi0)) * (Math.cos(phi + phi0)))
      + ((((15.0 / 8.0) * (n * n)) + ((15.0 / 8.0) * (n * n * n))) * (Math.sin(2 * (phi - phi0))) * (Math.cos(2 * (phi + phi0))))
      - (((35.0 / 24.0) * (n * n * n)) * (Math.sin(3 * (phi - phi0))) * (Math.cos(3 * (phi + phi0)))))
    MarcVal
  }
}
