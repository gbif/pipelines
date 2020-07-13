package au.org.ala.pipelines.vocabulary;

import java.io.FileNotFoundException;

import au.org.ala.kvs.LocationInfoConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Load centres of countries from resources
 */
@Slf4j
public class CountryCentrePoints {

  private static CentrePoints cp;

  public static CentrePoints getInstance(LocationInfoConfig config) throws FileNotFoundException {
    if (cp == null) {
      String externalFilePath = null;
      if (config != null){
        externalFilePath = config.getCountryCentrePointsFile();
      }
      cp = CentrePoints.getInstance(externalFilePath, "/countryCentrePoints.txt");
      log.info("CountryCentrePoints contains {} country centres", cp.size());
    }
    return cp;
  }
}
