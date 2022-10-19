package au.org.ala.pipelines.vocabulary;

import au.org.ala.kvs.LocationInfoConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Load centres of countries from resources */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CountryCentrePoints {
  private static final String CLASSPATH_FILE = "/countryCentrePoints.txt";
  private static CentrePoints cp;

  private static final Object MUTEX = new Object();

  public static CentrePoints getInstance(LocationInfoConfig config) throws FileNotFoundException {
    if (cp == null) {
      InputStream is = loadCentrePoints(config);
      cp = CentrePoints.getInstance(is, "COUNTRY");
      log.info("We found {} country centres", cp.size());
    }
    return cp;
  }

  @VisibleForTesting
  static InputStream loadCentrePoints(LocationInfoConfig config) throws FileNotFoundException {
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
    return is;
  }
}
