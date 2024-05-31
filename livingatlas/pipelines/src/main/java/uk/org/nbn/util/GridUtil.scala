package uk.org.nbn.util

import com.google.common.cache.CacheBuilder
import org.apache.commons.lang.StringUtils
import org.geotools.referencing.CRS
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer



/**
  * Utilities for parsing UK Ordnance survey British and Irish grid references.
  */
object GridUtil {

  import JavaConversions._

  val logger = LoggerFactory.getLogger("GridUtil")

  private val lock : AnyRef = new Object()
  val lru = CacheBuilder.newBuilder().maximumSize(100000).build[String, Option[GISPoint]]()

  //deal with the 2k OS grid ref separately
  val osGridRefNoEastingNorthing = ("""([A-Z]{2})""").r
  val osGridRefRegex1Number = """([A-Z]{2})\s*([0-9]+)$""".r
  val osGridRef50kRegex = """([A-Z]{2})\s*([NW|NE|SW|SE]{2})$""".r
  val osGridRef2kRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([A-Z]{1})""".r
  val osGridRefRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)$""".r
  val osGridRefWithQuadRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([NW|NE|SW|SE]{2})$""".r

  //deal with the 2k OS grid ref separately
  val irishGridletterscodes = Array('A','B','C','D','F','G','H','J','L','M','N','O','Q','R','S','T','V','W','X','Y')
  val irishGridlettersFlattened = irishGridletterscodes.mkString
  val irishGridRefNoEastingNorthing = ("""(I?[""" + irishGridlettersFlattened +"""]{1})""").r
  val irishGridRefRegex1Number = """(I?[A-Z]{1})\s*([0-9]+)$""".r
  val irishGridRef50kRegex = """([A-Z]{1})\s*([NW|NE|SW|SE]{2})$""".r
  val irishGridRef2kRegex = """(I?[A-Z]{1})\s*([0-9]+)\s*([0-9]+)\s*([A-Z]{1})""".r
  val irishGridRefRegex = """(I?[A-Z]{1})\s*([0-9]+)\s*([0-9]+)$""".r
  val irishGridRefWithQuadRegex = """(I?[A-Z]{1})\s*([0-9]+)\s*([0-9]+)\s*([NW|NE|SW|SE]{2})$""".r
  val tetradLetters = Array('A','B','C','D','E','F','G','H','I','J','K','L','M','N','P','Q','R','S','T','U','V','W','X','Y','Z')

  //CRS
  val IRISH_CRS = "EPSG:29902"
  val OSGB_CRS = "EPSG:27700"

  //proportion of grid size a point is allowed to vary (in x or y dimensions) from the true centre and still be considered central (to deal with rounding errors)
  val CENTROID_FRACTION = 0.1

  lazy val crsEpsgCodesMap = {
    var valuesMap = Map[String, String]()
    for (line <- scala.io.Source.fromURL(getClass.getResource("/crsEpsgCodes.txt"), "utf-8").getLines().toList) {
      val values = line.split('=')
      valuesMap += (values(0) -> values(1))
    }
    valuesMap
  }

  lazy val zoneEpsgCodesMap = {
    var valuesMap = Map[String, String]()
    for (line <- scala.io.Source.fromURL(getClass.getResource("/zoneEpsgCodes.txt"), "utf-8").getLines().toList) {
      val values = line.split('=')
      valuesMap += (values(0) -> values(1))
    }
    valuesMap
  }
  /**
    * Derive a value from the grid reference accuracy for grid size.
    *
    * @param noOfNumericalDigits
    * @param noOfSecondaryAlphaChars
    * @return
    */
  def getGridSizeFromGridRef(noOfNumericalDigits:Int, noOfSecondaryAlphaChars:Int) : Option[Int] = {
    val accuracy = noOfNumericalDigits match {
      case 10 => 1
      case 8 => 10
      case 6 => 100
      case 4 => 1000
      case 2 => 10000
      case 0 => 100000
      case _ => return None
    }
    noOfSecondaryAlphaChars match {
      case 2 => Some(accuracy / 2)
      case 1 => Some(accuracy / 5)
      case _ => Some(accuracy)
    }
  }

  /**
   * Reduce the resolution of the supplied grid reference.
   *
   * @param gridReference
   * @param uncertaintyString
   * @return
   */
  def convertReferenceToResolution(gridReference:String, uncertaintyString:String) : Option[String] = {

    try {
      val gridRefs = getGridRefAsResolutions(gridReference)
      val uncertainty = uncertaintyString.toInt

      val gridRefSeq = Array(
        gridRefs.getOrElse("grid_ref_100000", ""),
        gridRefs.getOrElse("grid_ref_50000", ""),
        gridRefs.getOrElse("grid_ref_10000", ""),
        gridRefs.getOrElse("grid_ref_2000", ""),
        gridRefs.getOrElse("grid_ref_1000", ""),
        gridRefs.getOrElse("grid_ref_100", "")
      )

      val ref = {
        if (uncertainty > 50000) {
          getBestValue(gridRefSeq, 0)
        } else if (uncertainty <= 50000 && uncertainty > 10000) {
          getBestValue(gridRefSeq, 1)
        } else if (uncertainty <= 10000 && uncertainty > 2000) {
          getBestValue(gridRefSeq, 2)
        } else if (uncertainty <= 2000 && uncertainty > 1000) {
          getBestValue(gridRefSeq, 3)
        } else if (uncertainty <= 1000 && uncertainty > 100) {
          getBestValue(gridRefSeq, 4)
        } else if (uncertainty <= 100) {
          getBestValue(gridRefSeq, 5)
        } else {
          ""
        }
      }
      if(ref != "")
        Some(ref)
      else
        None
    } catch {
      case e:Exception => {
        logger.error("Problem converting grid reference " + gridReference + " to lower resolution of " + uncertaintyString, e)
        None
      }
    }
  }


  def getBestValue(values: Seq[String], preferredIndex:Int): String ={

    var counter = preferredIndex
    while(counter>=0){
      if(values(counter) != ""){
        return values(counter)
      }
      counter = counter -1
    }
    ""
  }

  private def padWithZeros(ref:String, pad:Int) = ("0" *  (pad - ref.length)) + ref

  /**
    * Takes a grid reference and returns a map of grid references at different resolutions.
    * Map will look like:
    * grid_ref_100000 -> "NO"
    * grid_ref_50000 -> "NOSW"
    * grid_ref_10000 -> "NO11"
    * grid_ref_1000 -> "NO1212"
    * grid_ref_100 -> "NO123123"
    *
    * @param gridRef
    * @return
    */
  def getGridRefAsResolutions(gridRef:String) : java.util.Map[String, String] = {

    val map = new util.HashMap[String, String]

    gridReferenceToEastingNorthing(gridRef) match {
      case Some(gr) => {

        val gridSize = gr.gridSize.getOrElse(-1)
        map.put("grid_ref_100000", gr.gridLetters)

          if (gridRef.length > 2) {

            val eastingAsStr = padWithZeros((gr.easting.toInt % 100000).toString, 5)
            val northingAsStr = padWithZeros((gr.northing.toInt % 100000).toString, 5)

            //add grid reference for 50km
            if (eastingAsStr.length() >= 2 && northingAsStr.length() >= 2) {
              var quad = ""
              if (eastingAsStr.substring(0, 1).toInt < 5) { //W
                if (northingAsStr.substring(0, 1).toInt < 5) { //S
                  quad = "SW"
                } else { //N
                  quad = "NW"
                }
              } else { //E
                if (northingAsStr.substring(0, 1).toInt < 5) { //S
                  quad = "SE"
                } else { //N
                  quad = "NE"
                }
              }
              map.put("grid_ref_50000", gr.gridLetters + quad)
            }

            if (gridSize < 50000) {
              //add grid references for 10km, and 1km
              if (eastingAsStr.length() >= 2 && northingAsStr.length() >= 2) {
                map.put("grid_ref_10000", gr.gridLetters + eastingAsStr.substring(0, 1) + northingAsStr.substring(0, 1))
              }
              if (eastingAsStr.length() >= 3 && northingAsStr.length() >= 3) {
                val eastingWithin10km = eastingAsStr.substring(1, 2).toInt
                val northingWithin10km = northingAsStr.substring(1, 2).toInt
                val tetrad = tetradLetters((eastingWithin10km / 2) * 5 + (northingWithin10km / 2))

                if (gridSize != -1 && gridSize <= 2000) {
                  map.put("grid_ref_2000", gr.gridLetters + eastingAsStr.substring(0, 1) + northingAsStr.substring(0, 1) + tetrad)
                }
                if (gridSize != -1 && gridSize <= 1000) {
                  map.put("grid_ref_1000", gr.gridLetters + eastingAsStr.substring(0, 2) + northingAsStr.substring(0, 2))
                }
              }

              if (gridSize != -1 && gridSize <= 100 && eastingAsStr.length > 3) {
                map.put("grid_ref_100", gr.gridLetters + eastingAsStr.substring(0, 3) + northingAsStr.substring(0, 3))
              }
            }
          }
      }
      case None => //do nothing
    }
    map
  }

  /**
   * Takes a grid reference (british or irish) and returns easting, northing, datum and precision.
   */
  def gridReferenceToEastingNorthing(gridRef:String): Option[GridRef] = {
    val result = osGridReferenceToEastingNorthing(gridRef)
    if(!result.isEmpty){
      result
    } else {
      irishGridReferenceToEastingNorthing(gridRef)
    }
  }

  def getGridSizeInMeters(gridRef:String): Option[Int] = {
    val grid = gridReferenceToEastingNorthing(gridRef)
    grid match {
      case Some(gr) => {
        gr.gridSize
      }
      case None => {
        logger.info("Invalid grid reference: " + gridRef)
        None
      }
    }
  }

  def isCentroid(decimalLongitude:Double, decimalLatitude:Double, gridRef:String): Boolean = {
    val grid = gridReferenceToEastingNorthing(gridRef)
    grid match {
      case Some(gr) => {
        val reposition = if(!gr.gridSize.isEmpty && gr.gridSize.get > 0){
          gr.gridSize.get / 2
        } else {
          0
        }

        val coordsCentroid = GISUtil.reprojectCoordinatesToWGS84(gr.easting + reposition, gr.northing + reposition, gr.datum, 5)
        val coordsCorner = GISUtil.reprojectCoordinatesToWGS84(gr.easting, gr.northing, gr.datum, 5)
        val (gridCentroidLatitude, gridCentroidLongitude) = coordsCentroid.get
        val (gridCornerLatitude, gridCornerLongitude) = coordsCorner.get
        val devLatitude = (decimalLatitude - gridCentroidLatitude.toDouble).abs
        val devLongitude = (decimalLongitude - gridCentroidLongitude.toDouble).abs
        val gridSizeLatitude = (gridCentroidLatitude.toDouble - gridCornerLatitude.toDouble).abs * 2.0
        val gridSizeLongitude = (gridCentroidLongitude.toDouble - gridCornerLongitude.toDouble).abs * 2.0
        if ((devLatitude > CENTROID_FRACTION * gridSizeLatitude) ||
          (devLongitude > CENTROID_FRACTION * gridSizeLongitude)) {
          false
        } else {
          true
        }
      }
      case None => {
        false
      }
    }
  }

  /**
    * Convert an ordnance survey grid reference to northing, easting and grid size.
    * This is a port of this javascript code:
    *
    * http://www.movable-type.co.uk/scripts/latlong-gridref.html
    *
    * with additional extensions to handle 2km grid references e.g. NM39A
    *
    * @param gridRef
    * @return easting, northing, coordinate uncertainty in meters, minEasting, minNorthing, maxEasting, maxNorthing, coordinate system
    *
    */
  def irishGridReferenceToEastingNorthing(gridRef:String): Option[GridRef] = {

    // validate & parse format
    val (gridletters:String, easting:String, northing:String, twoKRef:String, quadRef:String, gridSize:Option[Int]) = gridRef.trim() match {
      case irishGridRefRegex1Number(gridletters, oneNumber) => {
        val gridDigits = oneNumber.toString
        val en = Array(gridDigits.substring(0, gridDigits.length / 2), gridDigits.substring(gridDigits.length / 2))
        val gridSize = getGridSizeFromGridRef(gridDigits.length, 0)
        (gridletters, en(0), en(1), "", "", gridSize)
      }
      case irishGridRefRegex(gridletters, easting, northing) => {
        (gridletters, easting, northing, "", "", getGridSizeFromGridRef(easting.length * 2, 0))
      }
      case irishGridRef2kRegex(gridletters, easting, northing, twoKRef) => {
        (gridletters, easting, northing, twoKRef, "", getGridSizeFromGridRef(easting.length * 2, 1))
      }
      case irishGridRefWithQuadRegex(gridletters, easting, northing, quadRef) => {
        (gridletters, easting, northing, "", quadRef, getGridSizeFromGridRef(easting.length * 2, 2))
      }
      case irishGridRefNoEastingNorthing(gridletters) => {
        (gridletters, "0", "0", "",  "", getGridSizeFromGridRef(0,0))
      }
      case irishGridRef50kRegex(gridletters, quadRef) => {
        (gridletters, "0", "0", "", quadRef, getGridSizeFromGridRef(0, 2))
      }
      case _ => return None
    }

    val singleGridLetter = if(gridletters.length == 2) gridletters.charAt(1) else gridletters.charAt(0)

    val gridIdx = irishGridletterscodes.indexOf(singleGridLetter)

    // convert grid letters into 100km-square indexes from false origin (grid square SV):
    val e100km = (gridIdx % 4)
    val n100km = (4 - (gridIdx / 4))

    val easting10digit = (easting + "00000").substring(0, 5)
    val northing10digit = (northing + "00000").substring(0, 5)

    var e = (e100km.toString + easting10digit).toInt
    var n = (n100km.toString + northing10digit).toInt

    /** C & P from below **/

    //handle the non standard grid parts
    if(twoKRef != ""){

      val cellSize = {
        if (easting.length == 1) 2000
        else if (easting.length == 2) 200
        else if (easting.length == 3) 20
        else if (easting.length == 4) 2
        else 0
      }

      //Dealing with 5 character grid references = 2km grids
      //http://www.kmbrc.org.uk/recording/help/gridrefhelp.php?page=6
      twoKRef match {
        case it if (Character.codePointAt(twoKRef, 0) <= 'N') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 65) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 65) % 5) * cellSize)
        }
        case it if (Character.codePointAt(twoKRef, 0) >= 'P') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 66) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 66) % 5) * cellSize)
        }
        case _ => return None
      }
    } else if(quadRef != ""){

      var cellSize = {
        if (easting.length == 1) 5000
        else if (easting.length == 2) 500
        else if (easting.length == 3) 50
        else if (easting.length == 4) 5
        else 0
      }
      if (gridSize.getOrElse(0) == 50000) { //50km grids only
        cellSize = 50000
      }
      if(cellSize > 0) {
        quadRef match {
          case "NW" => {
            e = e //+ (cellSize / 2)
            n = n + (cellSize) // + cellSize / 2)
          }
          case "NE" => {
            e = e + (cellSize) // + cellSize / 2)
            n = n + (cellSize) // + cellSize / 2)
          }
          case "SW" => {
            e = e //+ (cellSize / 2)
            n = n //+ (cellSize / 2)
          }
          case "SE" => {
            e = e + (cellSize) // + cellSize / 2)
            n = n //+ (cellSize / 2)
          }
          case _ => return None
        }
      }
    }

    /** end of C & P ***/
    val gridSizeOrZero = if(gridSize.isEmpty) 0 else gridSize.get

    Some(GridRef(gridletters, e, n, Some(gridSizeOrZero), e, n, e + gridSizeOrZero, n + gridSizeOrZero, IRISH_CRS))
  }

  /**
    * Convert an ordnance survey grid reference to northing, easting and grid size.
    * This is a port of this javascript code:
    *
    * http://www.movable-type.co.uk/scripts/latlong-gridref.html
    *
    * with additional extensions to handle 2km grid references e.g. NM39A
    *
    * ADDED: handling for 50km grids like 'SK SE' (although NBN won't accept data in this format, these grids can be assigned for sensitive records with 50km generalisations)
    *
    * @param gridRef
    * @return easting, northing, grid size in meters, minEasting, minNorthing, maxEasting, maxNorthing
    */
  def osGridReferenceToEastingNorthing(gridRef:String): Option[GridRef] = {

    //deal with the 2k OS grid ref separately
    val osGridRefNoEastingNorthing = ("""([A-Z]{2})""").r
    val osGridRef50kRegex = """([A-Z]{2})\s*([NW|NE|SW|SE]{2})$""".r
    val osGridRefRegex1Number = """([A-Z]{2})\s*([0-9]+)$""".r
    val osGridRef2kRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([A-Z]{1})""".r
    val osGridRefRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)$""".r
    val osGridRefWithQuadRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([NW|NE|SW|SE]{2})$""".r

    // validate & parse format
    val (gridletters:String, easting:String, northing:String, twoKRef:String, quadRef:String, gridSize:Option[Int]) = gridRef.toUpperCase().trim() match {
      case osGridRefRegex1Number(gridletters, oneNumber) => {
        val gridDigits = oneNumber.toString
        val en = Array(gridDigits.substring(0, gridDigits.length / 2), gridDigits.substring(gridDigits.length / 2))
        val gridSize = getGridSizeFromGridRef(gridDigits.length, 0)
        (gridletters, en(0), en(1), "", "", gridSize)
      }
      case osGridRefRegex(gridletters, easting, northing) => {
        (gridletters, easting, northing, "", "", getGridSizeFromGridRef(easting.length * 2, 0))
      }
      case osGridRef2kRegex(gridletters, easting, northing, twoKRef) => {
        (gridletters, easting, northing, twoKRef, "", getGridSizeFromGridRef(easting.length * 2, 1))
      }
      case osGridRefWithQuadRegex(gridletters, easting, northing, quadRef) => {
        (gridletters, easting, northing, "", quadRef, getGridSizeFromGridRef(easting.length * 2, 2))
      }
      case osGridRefNoEastingNorthing(gridletters) => {
        (gridletters, "0", "0", "",  "", getGridSizeFromGridRef(0,0))
      }
      case osGridRef50kRegex(gridletters, quadRef) => {
        (gridletters, "0", "0", "", quadRef, getGridSizeFromGridRef(0, 2))
      }
      case _ => return None
    }

    // get numeric values of letter references, mapping A->0, B->1, C->2, etc: (skipping I)
    val l1 = {
      val value = Character.codePointAt(gridletters, 0) - Character.codePointAt("A", 0)
      if(value > 7){
        value - 1
      } else {
        value
      }
    }
    val l2 = {
      val value = Character.codePointAt(gridletters, 1) - Character.codePointAt("A", 0)
      if(value > 7){
        value - 1
      } else {
        value
      }
    }

    // convert grid letters into 100km-square indexes from false origin (grid square SV):
    val e100km = (((l1-2) % 5) * 5 + (l2 % 5)).toInt
    val n100km = ((19 - Math.floor(l1 / 5) * 5) - Math.floor(l2 / 5)).toInt

    // validation
    if (e100km<0 || e100km>6 || n100km<0 || n100km>12) {
      return None
    }
    if (easting == null || northing == null){
      return None
    }
    if (easting.length() != northing.length()){
      return None
    }

    // standardise to 10-digit refs (metres)
    val easting10digit = (easting + "00000").substring(0, 5)
    val northing10digit = (northing + "00000").substring(0, 5)

    var e = (e100km.toString + easting10digit).toInt
    var n = (n100km.toString + northing10digit).toInt

    //handle the non standard grid parts
    if(twoKRef != ""){

      val cellSize = {
        if (easting.length == 1) 2000
        else if (easting.length == 2) 200
        else if (easting.length == 3) 20
        else if (easting.length == 4) 2
        else 0
      }

      //Dealing with 5 character grid references = 2km grids
      //http://www.kmbrc.org.uk/recording/help/gridrefhelp.php?page=6
      twoKRef match {
        case it if (Character.codePointAt(twoKRef, 0) <= 'N') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 65) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 65) % 5) * cellSize)
        }
        case it if (Character.codePointAt(twoKRef, 0) >= 'P') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 66) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 66) % 5) * cellSize)
        }
        case _ => return None
      }
    } else if(quadRef != ""){

      var cellSize = {
        if (easting.length == 1) 5000
        else if (easting.length == 2) 500
        else if (easting.length == 3) 50
        else if (easting.length == 4) 5
        else 0
      }
      if (gridSize.getOrElse(0) == 50000) { //50km grids only
        cellSize = 50000
      }
      if(cellSize > 0) {
        quadRef match {
          case "NW" => {
            e = e //+ (cellSize / 2)
            n = n + (cellSize) // + cellSize / 2)
          }
          case "NE" => {
            e = e + (cellSize) // + cellSize / 2)
            n = n + (cellSize) // + cellSize / 2)
          }
          case "SW" => {
            e = e //+ (cellSize / 2)
            n = n //+ (cellSize / 2)
          }
          case "SE" => {
            e = e + (cellSize) // + cellSize / 2)
            n = n //+ (cellSize / 2)
          }
          case _ => return None
        }
      }
    }

    val gridSizeOrZero = if(gridSize.isEmpty) 0 else gridSize.get

    Some(GridRef(gridletters, e, n, gridSize, e, n, e + gridSizeOrZero, n + gridSizeOrZero, OSGB_CRS))
  }

/**
  * Convert a WGS84 lat/lon coordinate to either OSGB (ordnance survey GB) or Irish OS grid reference using coordinateUncertaintyInMeters to define grid cell size
  *
  * Note: does not handle 2000m uncertainty
  *
  *  http://www.carabus.co.uk/ll_ngr.html
  *
  * @param lat latitude
  * @param lon longitude
  * @param coordinateUncertaintyInMeters
  * @param geodeticDatum geodeticDatum (if empty assume WGS84)
  * @return gridRef
  */
  def latLonToOsGrid (lat:Double, lon:Double, coordinateUncertaintyInMeters:Double, geodeticDatum:String, gridType:String, knownGridSize: Int = -1): Option[String] = {
    val datum = lookupEpsgCode(geodeticDatum)

    var N = 0.0
    var E = 0.0

    if (!(datum == Some("") || datum == Some("EPSG:27700") || datum == Some("EPSG:4326"))) {
      return None
    } else {
      if (datum == Some("EPSG:27700")) {
        //in OSGB36
        val northingsEastings = GridRefGISUtil.coordinatesOSGB36toNorthingEasting(lat, lon, 4)
        val (northings, eastings) = northingsEastings.get
        N = northings.toDouble
        E = eastings.toDouble
      } else { //assume WGS84
        val reprojectedNorthingsEastings = gridType match {
          case "OSGB" => GridRefGISUtil.reprojectCoordinatesWGS84ToOSGB36(lat, lon, 4)
          case "Irish" => GridRefGISUtil.reprojectCoordinatesWGS84ToOSNI(lat, lon, 4)
          case _ => GridRefGISUtil.reprojectCoordinatesWGS84ToOSGB36(lat, lon, 4)
        }
        val (northings, eastings) = reprojectedNorthingsEastings.get
        N = northings.toDouble
        E = eastings.toDouble
      }
    }

    val gridSize = {
        if (knownGridSize >= 0) {
          gridSizeRoundedUp(knownGridSize)
        } else {
          calculateGridSize(coordinateUncertaintyInMeters)
        }
    }

    val digits = calculateNumOfGridRefDigits(gridSize)

    if (gridSize == 2000) {
      //FIXME: sort out getOSGridFromNorthingEasting to handle 2km, 50km grids properly
      val onekmGrid = getOSGridFromNorthingEasting(Math.round(N), Math.round(E), 4, gridType)
      //now convert 1km grid to containing 2km grid
      if (onekmGrid.isDefined) {
        convertReferenceToResolution(onekmGrid.get, "2000")
      } else {
        None
      }
    } else if (gridSize == 50000) {
      //FIXME: sort out getOSGridFromNorthingEasting to handle 2km, 50km grids properly
      val tenkmGrid = getOSGridFromNorthingEasting(Math.round(N), Math.round(E), 2, gridType)
      //now convert 10km grid to containing 50km grid
      if (tenkmGrid.isDefined) {
        convertReferenceToResolution(tenkmGrid.get, "50000")
      } else {
        None
      }
    } else {
      getOSGridFromNorthingEasting(Math.round(N), Math.round(E), digits, gridType)
    }

  }

  def calculateGridSize(coordinateUncertaintyInMeters: Double): Int ={
    return gridSizeRoundedUp{
        coordinateUncertaintyInMeters * math.sqrt(2.0) - 0.001 //old coordinateUncertaintyInMeters was understood as the linear dimension of a grid cell - convert back to this from a centre to corner distance
        //danger here is rounding error from floating point arithmetic, hence the arbitrary deduction
      }
  }

  def calculateNumOfGridRefDigits(gridSize: Int): Int = {
    val digits = gridSize match {
      case 1 => 10
      case 10 => 8
      case 100 => 6
      case 1000 => 4
      case 2000 => 3
      case 10000 => 2
      case _ =>  0
    }
    digits
  }

  private def gridSizeRoundedUp(gridSize: Double): Int = {
    val limits = Array(1, 10, 100, 1000, 2000, 10000, 100000)
    val gridSizeWhole = Math.round(gridSize)

    for (limit <- limits) {
      if (gridSizeWhole <= limit) {
       return limit
      }
    }

    100000
  }


  /**
    *
    * @param n : north
    * @param e : east
    * @param digits : digits
    * @param gridType : gridType (Irish or OSGB)
    * @return
    */
  def getOSGridFromNorthingEasting(n:Double, e:Double, digits:Int, gridType: String): Option[String] = {
    if ((digits % 2 != 0) || digits > 16) {
      return None
    } else {
      // get the 100km-grid indices// get the 100km-grid indices

      val e100k = Math.floor(e / 100000)
      val n100k = Math.floor(n / 100000)

      if (e100k < 0 || e100k > 6 || n100k < 0 || n100k > 12) {
        return None
      }
      // translate those into numeric equivalents of the grid letters// translate those into numeric equivalents of the grid letters
      var l1 = (19 - n100k) - (19 - n100k) % 5 + Math.floor((e100k + 10) / 5)
      var l2 = (19 - n100k) * 5 % 25 + e100k % 5
      // compensate for skipped 'I' and build grid letter-pairs// compensate for skipped 'I' and build grid letter-pairs
      if (l1 > 7) {
        l1 += 1
      }
      if (l2 > 7) {
        l2 += 1
      }
      val letterPair = (l1 + Character.codePointAt("A", 0)).toChar.toString.concat((l2 + Character.codePointAt("A", 0)).toChar.toString)
      val letterPairLastOnly = ((l2 + Character.codePointAt("A", 0)).toChar.toString)
      // strip 100km-grid indices from easting & northing, and reduce precision
      val eMod = Math.floor((e % 100000) / Math.pow(10, 5 - digits / 2))
      val nMod = Math.floor((n % 100000) / Math.pow(10, 5 - digits / 2))
      // pad eastings & northings with leading zeros (just in case, allow up to 16-digit (mm) refs)
      var eModStr = padWithZeros(eMod.round.toString(),8).takeRight(digits/2)
      var nModStr = padWithZeros(nMod.round.toString(),8).takeRight(digits/2)

      gridType match {
        case "Irish" => Some(letterPairLastOnly.concat(eModStr).concat(nModStr))
        case "OSGB" => Some(letterPair.concat(eModStr).concat(nModStr))
        case _ => Some(letterPair.concat(eModStr).concat(nModStr)) //default
      }
    }
  }

  /**
    * Process supplied grid references. This currently only recognises UK OS grid references but could be
    * extended to support other systems.
    *
    * @param gridReference
    */
  def processGridReference(gridReference:String): Option[GISPoint] = {

    val cachedObject = lru.getIfPresent(gridReference)
    if(cachedObject != null)
      return cachedObject.asInstanceOf[Option[GISPoint]]


    val result = GridUtil.gridReferenceToEastingNorthing(gridReference) match {
      case Some(gr) => {

        //move coordinates to the centroid of the grid
        val reposition = if(!gr.gridSize.isEmpty && gr.gridSize.get > 0){
          gr.gridSize.get / 2
        } else {
          0
        }

        val coords = GISUtil.reprojectCoordinatesToWGS84(gr.easting + reposition, gr.northing + reposition, gr.datum, 6)

        //reproject min/max lat/lng
        val bbox = Array(
          GISUtil.reprojectCoordinatesToWGS84(gr.minEasting, gr.minNorthing, gr.datum, 6),
          GISUtil.reprojectCoordinatesToWGS84(gr.maxEasting, gr.maxNorthing, gr.datum, 6)
        )

        if(!coords.isEmpty){
          val (latitude, longitude) = coords.get
          val uncertaintyToUse = if(!gr.gridSize.isEmpty){
            "%.1f".format(gr.gridSize.get / math.sqrt(2.0))
          } else {
            null
          }
          Some(GISPoint(
            latitude,
            longitude,
            GISUtil.WGS84_EPSG_Code,
            uncertaintyToUse,
            easting = gr.easting.toString,
            northing = gr.northing.toString,
            minLatitude = bbox(0).get._1,
            minLongitude = bbox(0).get._2,
            maxLatitude = bbox(1).get._1,
            maxLongitude = bbox(1).get._2
          ))
        } else {
          None
        }
      }
      case None => None
    }
    lru.put(gridReference, result)
    result
  }

  /**
    * Get the EPSG code associated with a coordinate reference system string e.g. "WGS84" or "AGD66".
    *
    * @param crs The coordinate reference system string.
    * @return The EPSG code associated with the CRS, or None if no matching code could be found.
    *         If the supplied string is already a valid EPSG code, it will simply be returned.
    */
   def lookupEpsgCode(crs: String): Option[String] = {
    if (StringUtils.startsWithIgnoreCase(crs, "EPSG:")) {
      // Do a lookup with the EPSG code to ensure that it is valid
      try {
        CRS.decode(crs.toUpperCase)
        // lookup was successful so just return the EPSG code
        Some(crs.toUpperCase)
      } catch {
        case ex: Exception => None
      }
    } else if (crsEpsgCodesMap.contains(crs.toUpperCase)) {
      Some(crsEpsgCodesMap(crs.toUpperCase()))
    } else {
      None
    }
  }

  // get grid reference as human readable text with an annotation to say if it's a OSGB or OSI grid
  def getGridAsTextWithAnnotation(gridReference: String = "") = {
    var text = ""
    if (gridReference != "" && gridReference != null) {
      // see if it's OSGB
      val result = GridUtil.osGridReferenceToEastingNorthing( gridReference )
      if(!result.isEmpty){
        text = "OSGB Grid Reference " + gridReference;
      } else {
        // otherwise assume it's OSI
        text = "OSI Grid Reference " + gridReference
      }
    }
    text
  }

  // get grid WKT from a grid reference
  def getGridWKT(gridReference: String = "") = {
    var poly_grid = ""
    if (gridReference != "") {
      GridUtil.gridReferenceToEastingNorthing(gridReference) match {
        case Some(gr) => {
          val bbox = Array(
            GISUtil.reprojectCoordinatesToWGS84(gr.minEasting, gr.minNorthing, gr.datum, 5),
            GISUtil.reprojectCoordinatesToWGS84(gr.maxEasting, gr.maxNorthing, gr.datum, 5)
          )
          val minLatitude = bbox(0).get._1
          val minLongitude = bbox(0).get._2
          val maxLatitude = bbox(1).get._1
          val maxLongitude = bbox(1).get._2

          //for WKT, need to give points in lon-lat order, not lat-lon
          poly_grid = "POLYGON((" + minLongitude + " " + minLatitude + "," +
            minLongitude + " " + maxLatitude + "," +
            maxLongitude + " " + maxLatitude + "," +
            maxLongitude + " " + minLatitude + "," +
            minLongitude + " " + minLatitude + "))"
        }
        case None => {
          logger.info("Invalid grid reference: " + gridReference)
        }
      }
    }
    poly_grid
  }


  /**
    * Converts a easting northing to a decimal latitude/longitude.
    *
    * @param verbatimSRS
    * @param easting
    * @param northing
    * @param zone
    * @param assertions
    * @return 3-tuple reprojectedLatitude, reprojectedLongitude, WGS84_EPSG_Code
    */
  //TODO KR what to do with assertions
//  def processNorthingEastingZone(verbatimSRS: String, easting: String, northing: String, zone: String,
//                                         assertions: ArrayBuffer[QualityAssertion]): Option[GISPoint] = {
//
//    // Need a datum and a zone to get an epsg code for transforming easting/northing values
//    val epsgCodeKey = {
//      if (verbatimSRS != null) {
//        verbatimSRS.toUpperCase + "|" + zone
//      } else {
//        // Assume GDA94 / MGA zone
//        "GDA94|" + zone
//      }
//    }
//
//    if (zoneEpsgCodesMap.contains(epsgCodeKey)) {
//      val crsEpsgCode = zoneEpsgCodesMap(epsgCodeKey)
//      val eastingAsDouble = easting.toDoubleWithOption
//      val northingAsDouble = northing.toDoubleWithOption
//
//      if (!eastingAsDouble.isEmpty && !northingAsDouble.isEmpty) {
//        // Always round to 5 decimal places as easting/northing values are in metres and 0.00001 degree is approximately equal to 1m.
//        val reprojectedCoords = GISUtil.reprojectCoordinatesToWGS84(eastingAsDouble.get, northingAsDouble.get, crsEpsgCode, 5)
//        if (reprojectedCoords.isEmpty) {
//          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
//            "Transformation of verbatim easting and northing to WGS84 failed")
//          None
//        } else {
//          //lat and long from easting and northing did NOT fail:
//          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED, PASSED)
//          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATED_FROM_EASTING_NORTHING,
//            "Decimal latitude and longitude were calculated using easting, northing and zone.")
//          val (reprojectedLatitude, reprojectedLongitude) = reprojectedCoords.get
//          Some(GISPoint(reprojectedLatitude, reprojectedLongitude, GISUtil.WGS84_EPSG_Code, null))
//        }
//      } else {
//        None
//      }
//    } else {
//      if (verbatimSRS == null) {
//        assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
//          "Unrecognized zone GDA94 / MGA zone " + zone)
//      } else {
//        assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
//          "Unrecognized zone " + verbatimSRS + " / zone " + zone)
//      }
//      None
//    }
//  }

  /**
   * Helper function to calculate coordinate uncertainty for a given grid size
   *
   * @param gridSize
   * @return
   */
  def gridToCoordinateUncertaintyString(gridSize: Integer): String = {
    "%.1f".format(gridSize / math.sqrt(2.0))
  }
}
