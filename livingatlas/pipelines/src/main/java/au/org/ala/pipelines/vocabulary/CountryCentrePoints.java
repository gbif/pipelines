package au.org.ala.pipelines.vocabulary;

import au.org.ala.kvs.LocationInfoConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;

/** Load centres of countries from resources */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CountryCentrePoints {
  private static final String CLASSPATH_FILE = "/countryCentrePoints.txt";
  private static CentrePoints cp;

  public static CentrePoints getInstance(LocationInfoConfig config) throws FileNotFoundException {
    if (cp == null) {
      InputStream is;
      if (config != null) {
        String externalFilePath = config.getCountryCentrePointsFile();
        if (Strings.isNullOrEmpty(externalFilePath)) {
          is = CentrePoints.class.getResourceAsStream(CLASSPATH_FILE);
        } else {
          File externalFile = new File(externalFilePath);
          is = new FileInputStream(externalFile);
        }
      } else {
        is = CentrePoints.class.getResourceAsStream(CLASSPATH_FILE);
      }
      cp = CentrePoints.getInstance(is, "COUNTRY");
      log.info("We found {} country centres", cp.size());
    }
    return cp;
  }
}
