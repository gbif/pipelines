package au.org.ala.pipelines.vocabulary;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import lombok.extern.slf4j.Slf4j;

import org.apache.logging.log4j.util.Strings;
import org.gbif.kvs.geocode.LatLng;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * CentrePoints is used by countryCentres and stateProvinceCentres, so it can be a singleton.
 *
 * Singleton should be implemented in a conconcrete class like, countryCentres / StateCentres <p> Simulate
 * CentrePoints.scala Compare with predefined state centres in file Format: New South
 * Wales	-31.2532183	146.921099	-28.1561921	153.903718	-37.5052772	140.9992122 Rounded decimal of
 * predefined state centres based on precision of the given coordinates
 *
 * @author Bai187
 */
@Slf4j
public class CentrePoints {

  private Map<String, LatLng> statesCentre = new HashMap();
  private Map<String, BBox> statesBBox = new HashMap();

  private static CentrePoints cp;

  private CentrePoints() {
  }

  public static CentrePoints getInstance(String filePath, String classpathFile) throws FileNotFoundException {
      if (Strings.isNotEmpty(filePath)){
        InputStream is = new FileInputStream(new File(filePath));
        return getInstance(is);
      } else {
        return getInstance(CentrePoints.class.getResourceAsStream(classpathFile));
      }
  }

  public static CentrePoints getInstance(InputStream is) {
    cp = new CentrePoints();
    new BufferedReader(new InputStreamReader(is)).lines()
        .map(s -> s.trim())
        .filter(l -> l.split("\t").length == 7)
        .forEach(l -> {
          String[] ss = l.split("\t");
          String state = ss[0].toLowerCase();
          LatLng centre = new LatLng(Double.parseDouble(ss[1]), Double.parseDouble(ss[2]));
          BBox bbox = new BBox(Double.parseDouble(ss[3]), Double.parseDouble(ss[4]),
              Double.parseDouble(ss[5]), Double.parseDouble(ss[6]));
          cp.statesCentre.put(state, centre);
          cp.statesBBox.put(state, bbox);
        });

    return cp;
  }

  /**
   * Precision of coordinate is determined by the given lat and lng for example, given lat 14.39,
   * will only compare to the second decimal
   */
  public boolean coordinatesMatchCentre(String location, double decimalLatitude,
      double decimalLongitude) {

    LatLng supposedCentre = statesCentre.get(location.toLowerCase());
    if (supposedCentre != null) {
      int latDecPlaces = noOfDecimalPlace(decimalLatitude);
      int longDecPlaces = noOfDecimalPlace(decimalLongitude);

      //approximate the centre points appropriately
      double approximatedLat = round(supposedCentre.getLatitude(), latDecPlaces);
      double approximatedLong = round(supposedCentre.getLongitude(), longDecPlaces);

      // compare approximated centre point with supplied coordinates
      if (log.isDebugEnabled()) {
        log.debug("{} {} VS {} {}", decimalLatitude, decimalLongitude, approximatedLat, approximatedLong);
      }
      return approximatedLat == decimalLatitude && approximatedLong == decimalLongitude;
    } else {
      log.error("{} is not found in records", location);
      return false;
    }
  }

  /**
   * @return size of centres
   */
  public int size() {
    return statesCentre.size();
  }

  private double round(double number, int decimalPlaces) {
    if (decimalPlaces > 0) {
      int x = 1;
      for (int i = 0; i < decimalPlaces; i++) {
        x = x * 10;
      }
      return ((double) (Math.round(number * x))) / x;
    } else {
      return Math.round(number);
    }
  }

  private int noOfDecimalPlace(double number) {
    String numberString = String.valueOf(number);
    int decimalPointLoc = numberString.indexOf(".");
    if (decimalPointLoc < 0) {
      return 0;
    } else {
      return numberString.substring(decimalPointLoc + 1).length();
    }
  }
}

class BBox {

  private double xmin;
  private double xmax;
  private double ymin;
  private double ymax;

  public BBox(double a_x, double a_y, double b_x, double b_y) {
    xmin = Math.min(a_x, b_x);
    xmax = Math.max(a_x, b_x);
    ymin = Math.min(a_y, b_y);
    ymax = Math.max(a_y, b_y);
    sanity();
  }

  private void sanity() {
    if (xmin < -180.0) {
      xmin = -180.0;
    }
    if (xmax > 180.0) {
      xmax = 180.0;
    }
    if (ymin < -90.0) {
      ymin = -90.0;
    }
    if (ymax > 90.0) {
      ymax = 90.0;
    }
  }

  public void add(LatLng c) {
    add(c.getLongitude(), c.getLatitude());
  }

  /**
   * Extends this bbox to include the point (x, y)
   */
  public void add(double x, double y) {
    xmin = Math.min(xmin, x);
    xmax = Math.max(xmax, x);
    ymin = Math.min(ymin, y);
    ymax = Math.max(ymax, y);
    sanity();
  }

  public void add(BBox box) {
    add(box.getTopLeft());
    add(box.getBottomRight());
  }

  public LatLng getTopLeft() {
    return new LatLng(ymax, xmin);
  }

  public LatLng getBottomRight() {
    return new LatLng(ymin, xmax);
  }

  @Override
  public int hashCode() {
    return (int) (ymin * xmin);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BBox) {
      BBox b = (BBox) o;
      return b.xmax == xmax && b.ymax == ymax && b.xmin == xmin && b.ymin == ymin;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "[ x: " + xmin + " -> " + xmax +
        ", y: " + ymin + " -> " + ymax + " ]";
  }
}
