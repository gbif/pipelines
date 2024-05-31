package uk.org.nbn.util

case class GISPoint(latitude:String, longitude:String, datum:String, coordinateUncertaintyInMeters:String,
                    easting:String = null, northing:String  = null,
                    minLatitude:String = null, minLongitude:String = null,
                    maxLatitude:String = null, maxLongitude:String = null){

  def bboxString = {
    if(minLatitude !=null && minLongitude !=null && maxLatitude !=null && maxLongitude !=null) {
      minLatitude + "," + minLongitude + "," + maxLatitude + "," + maxLongitude
    } else {
      ""
    }
  }
}