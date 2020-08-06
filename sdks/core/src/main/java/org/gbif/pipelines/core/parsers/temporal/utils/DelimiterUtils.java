package org.gbif.pipelines.core.parsers.temporal.utils;

import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Contains methods which can delimit raw string into raw array, example: "10-10-2010" to {"10",
 * "10", "2010"} and etc.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DelimiterUtils {

  // Period values
  private static final String CHAR_REPLACE = " ";
  private static final Pattern RGX_FILTER = Pattern.compile("[^a-zA-Z0-9]+");
  private static final Pattern RGX_REPLACE_CHAR = Pattern.compile(CHAR_REPLACE);

  // DateTime values
  private static final char TIME_DELIMITER = ':';

  // Date values
  private static final String CHAR_PERIOD = "/";
  private static final Pattern RGX_PERIOD = Pattern.compile(CHAR_PERIOD);

  // Time values
  private static final Pattern RGX_TIME = Pattern.compile("[^\\d]");
  private static final Pattern RGX_MINUS = Pattern.compile("[-]");
  private static final Pattern RGX_PLUS = Pattern.compile("[+]");
  private static final Pattern RGX_Z = Pattern.compile("[zZ]");
  private static final Pattern RGX_ESCAPE = Pattern.compile("m");

  private static final String ESCAPE = "m";
  private static final String MINUS = ESCAPE + "-";
  private static final String PLUS = ESCAPE + "+";
  private static final String Z = ESCAPE + "Z";

  /**
   * Attempt to split the rawPeriod into raw periods by delimiter '/' symbol, the symbol must be the
   * only one in the rawPeriod
   *
   * @param rawPeriod raw string period dates
   * @return always two elements array
   */
  public static String[] splitPeriod(String rawPeriod) {
    // If the delimiter for the first position and last position are the same, symbol only one in
    // string,
    // check length, the length must be greater than seven to avoid case as "1999/2", where it looks
    // as year and month
    boolean canSplit =
        rawPeriod.lastIndexOf(CHAR_PERIOD) == rawPeriod.indexOf(CHAR_PERIOD)
            && rawPeriod.length() > 7;
    String[] splitted = canSplit ? RGX_PERIOD.split(rawPeriod) : new String[] {rawPeriod, ""};
    // Returns an array of the same length each time
    return splitted.length < 2 ? new String[] {splitted[0], ""} : splitted;
  }

  /**
   * Split raw date time into an array with two elements, example: "10-10-2010T10:10:10" to
   * {"10-10-2010", "10:10:10"}
   *
   * @param rawDateTime raw string date and time
   * @return an array with two elements, where fist is the date and second the time, example:
   *     {"10-10-2010", "10:10:10"}
   */
  public static String[] splitDateTime(String rawDateTime) {
    // Does value have time inside
    int timeDelimiterIdx = rawDateTime.indexOf(TIME_DELIMITER);

    // Calculate a shift index of the time, for case as 09:00 or 9:00
    int shift = -1;
    for (int x = timeDelimiterIdx - 1; x > 0; x--) {
      char sign = rawDateTime.charAt(x);
      if (!Character.isDigit(sign)) {
        shift = x;
        break;
      }
    }

    // Calculate the beginning index of date, the date can be absent, in this case, substring from 0
    // to 0 returns empty string
    int dateIdx = Math.max(shift, 0);
    boolean hasTime = timeDelimiterIdx > 0;
    String date = hasTime ? rawDateTime.substring(0, dateIdx) : rawDateTime;

    // Calculate the beginning index of time
    int timeIdx = shift + 1;
    String time = hasTime ? rawDateTime.substring(timeIdx) : "";

    return new String[] {date, time};
  }

  /**
   * Split raw date into an array with raw elements, example: "10-10-2010" to {"10", "10", "2010"}
   *
   * @param rawDate raw string date
   * @return an array with raw elements, example: {"10", "10", "2010"}
   */
  public static String[] splitDate(String rawDate) {
    String filtered = RGX_FILTER.matcher(rawDate).replaceAll(CHAR_REPLACE).trim();
    return RGX_REPLACE_CHAR.split(filtered);
  }

  /**
   * Split raw time into an array with raw elements, example: "10:10:10" to {"10", "10", "10"}
   *
   * @param rawTime raw string time
   * @return an array with two elements, where fist is the date and second the time
   */
  public static String[] splitTime(String rawTime) {
    return RGX_TIME.split(rawTime);
  }

  /**
   * Split raw time into an array with raw elements, example: "10:10:10+02:00" to {"10:10:10",
   * "+02:00"}
   *
   * @param rawTime raw string time
   * @return an array with two elements, where fist is the time and zone
   */
  public static String[] splitTimeAndZone(String rawTime) {
    String minus = RGX_MINUS.matcher(rawTime).replaceAll(MINUS);
    String plus = RGX_PLUS.matcher(minus).replaceAll(PLUS);
    String z = RGX_Z.matcher(plus).replaceAll(Z);

    return RGX_ESCAPE.split(z.trim());
  }
}
