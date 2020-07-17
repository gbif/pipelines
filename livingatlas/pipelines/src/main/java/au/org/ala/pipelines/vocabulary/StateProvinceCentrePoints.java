package au.org.ala.pipelines.vocabulary;

import au.org.ala.kvs.LocationInfoConfig;
import java.io.FileNotFoundException;
import lombok.extern.slf4j.Slf4j;

/** Load centres of stateProvince from resources */
@Slf4j
public class StateProvinceCentrePoints {

  private static CentrePoints cp;

  public static CentrePoints getInstance(LocationInfoConfig config) throws FileNotFoundException {
    if (cp == null) {
      String externalFilePath = null;
      if (config != null) {
        externalFilePath = config.getStateProvinceCentrePointsFile();
      }
      cp = CentrePoints.getInstance(externalFilePath, "/stateProvinceCentrePoints.txt");
      log.info("StateProvinceCentrePoints contains " + cp.size() + " stateProvince centres");
    }
    return cp;
  }
}
