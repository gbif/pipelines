package org.gbif.pipelines.core.parsers.temporal;

import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Contains methods which can delimit raw string into raw array, example: "10-10-2010" to {"10",
 * "10", "2010"} and etc.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DelimiterUtils {
  // Date values
  private static final String CHAR_PERIOD = "/";
  private static final Pattern RGX_PERIOD = Pattern.compile(CHAR_PERIOD);

  //  1990-01-02/15, and none-ISO: 1990-1-2, but does not support 19000101/12
  private static final String ISO_YMD_RANGE =
      "^([0-9]{4})-(1[0-2]|0[1-9]|[1-9])-(3[01]|0[1-9]|[12][0-9]|[1-9])/(3[01]|0[1-9]|[12][0-9]|[1-9])";
  // 1990-01/12, and none-ISO: 1990-1/2
  private static final String ISO_YM_RANGE =
      "^([0-9]{4})-(1[0-2]|0[1-9]|[1-9])/(1[0-2]|0[1-9]|[1-9])";
  //  1990-01-02/09-15, and none-ISO: 1990-1-2/9-15
  private static final String ISO_YMD_MD_RANGE =
      "^([0-9]{4})-(1[0-2]|0[1-9]|[1-9])-(3[01]|0[1-9]|[12][0-9]|[1-9])/(1[0-2]|0[1-9]|[1-9])-(3[01]|0[1-9]|[12][0-9]|[1-9])";

  /**
   * Attempt to split the rawPeriod into raw periods by delimiter '/' symbol, the symbol must be the
   * only one in the rawPeriod
   *
   * @param rawPeriod raw string period dates
   * @return always two elements array
   */
  public static String[] splitPeriod(String rawPeriod) {
    if (rawPeriod == null || rawPeriod.isEmpty()) {
      return new String[] {"", ""};
    }
    // todo can be improved by adding more date patterns
    String[] isoRange = splitISODateRange(rawPeriod);
    if (isoRange != null) {
      return isoRange;
    }
    // If the delimiter for the first position and last position are the same, symbol only one in
    // string,
    // check length, the length must be greater than seven to avoid case as "1999/2", where it looks
    // as year and month
    boolean canSplit =
        rawPeriod.lastIndexOf(CHAR_PERIOD) == rawPeriod.indexOf(CHAR_PERIOD)
            && rawPeriod.length() > 7;
    String[] splitted =
        canSplit ? RGX_PERIOD.split(rawPeriod) : new String[] {rawPeriod, rawPeriod};
    // Returns an array of the same length each time
    return splitted.length < 2 ? new String[] {splitted[0], splitted[0]} : splitted;
  }

  /** Handle day/month ranges Examples: 1991-1/3 : 1991-1, 1991-3 1991-1-2/5 : 1991-1-1, 1991-1-5 */
  public static String[] splitISODateRange(String src) {
    if (src.matches(ISO_YMD_RANGE) || src.matches(ISO_YM_RANGE)) {
      String[] periods = src.split(CHAR_PERIOD);
      String from = periods[0];
      String to = periods[1];
      String ym = from.substring(0, from.lastIndexOf('-')); // Get YM
      to = ym + "-" + to;
      return new String[] {from, to};
    } else if (src.matches(ISO_YMD_MD_RANGE)) {
      // 1990-01-02/09-15
      String[] periods = src.split(CHAR_PERIOD);
      String from = periods[0];
      String to = periods[1];
      String y = from.substring(0, from.indexOf('-')); // Get Y
      to = y + "-" + to;
      return new String[] {from, to};
    }
    return null;
  }
}
