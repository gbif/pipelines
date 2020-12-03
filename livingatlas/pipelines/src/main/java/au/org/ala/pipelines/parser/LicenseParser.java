package au.org.ala.pipelines.parser;

import com.google.common.base.Strings;
import com.google.common.collect.LinkedListMultimap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class LicenseParser {
  private static LicenseParser parser;
  // Licence 1..* regex (s)
  private final LinkedListMultimap<String, String> licences = LinkedListMultimap.create();

  /**
   * Create a ALA supported license parser
   *
   * @return
   */
  public static LicenseParser getInstance() {
    String sourceClasspathFile = "/license.txt";
    if (parser == null) {
      InputStream is = LicenseParser.class.getResourceAsStream(sourceClasspathFile);
      parser = LicenseParser.getInstance(is);
    }
    return parser;
  }

  /**
   * Load customised license pattern file.
   *
   * @param licenseFile: //CC-BY-NC-SA 3.0 (Au) .*(cc|creativecommons).*by.*nc.*sa.*3\.0.*au.*
   * @return
   * @throws FileNotFoundException
   */
  public static LicenseParser getInstance(String licenseFile) throws FileNotFoundException {
    if (parser == null) {
      InputStream is;
      File externalFile = new File(licenseFile);
      is = new FileInputStream(externalFile);
      parser = LicenseParser.getInstance(is);
    }
    return parser;
  }

  private static LicenseParser getInstance(InputStream is) {
    LicenseParser lp = new LicenseParser();
    new BufferedReader(new InputStreamReader(is))
        .lines()
        .filter(s -> !Strings.isNullOrEmpty(s))
        .map(String::trim)
        .filter(s -> !s.startsWith("#"))
        .filter(s -> s.contains("\t"))
        .forEach(
            l -> {
              String[] ss = l.split("\t");
              String key = ss[0];
              String regex = ss[1];
              lp.licences.put(key, regex);
            });
    return lp;
  }

  /**
   * Get the license title via regex match.
   *
   * <p>This method does not check if input is null
   *
   * @param input
   * @return
   */
  public String matchLicense(String input) {
    for (Map.Entry<String, String> licence : licences.entries()) {
      String name = licence.getKey();
      String regex = licence.getValue();
      // case insensitive
      if (input.matches("(?i).*" + regex)) {
        return name;
      }
    }
    return "Custom";
  }
}
