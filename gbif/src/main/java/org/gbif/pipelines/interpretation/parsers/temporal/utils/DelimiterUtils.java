package org.gbif.pipelines.interpretation.parsers.temporal.utils;

import java.util.regex.Pattern;

public class DelimiterUtils {

  // Period
  private static final String CHAR_REPLACE = " ";
  private static final Pattern RGX_FILTER = Pattern.compile("[^a-zA-Z0-9]+");
  private static final Pattern RGX_REPLACE_CHAR = Pattern.compile(CHAR_REPLACE);

  // DateTime
  private static final char TIME_DELIMITER= ':';

  // Date
  private static final String CHAR_PERIOD = "/";
  private static final Pattern RGX_PERIOD = Pattern.compile(CHAR_PERIOD);

  // Time
  private static final Pattern RGX_TIME = Pattern.compile("[^\\d]");
  private static final Pattern RGX_MINUS = Pattern.compile("[-]");
  private static final Pattern RGX_PLUS = Pattern.compile("[+]");
  private static final Pattern RGX_Z = Pattern.compile("[zZ]");
  private static final Pattern RGX_ESCAPE = Pattern.compile("m");

  private static final String ESCAPE = "m";
  private static final String MINUS = ESCAPE + "-";
  private static final String PLUS = ESCAPE + "+";
  private static final String Z = ESCAPE + "Z";

  private DelimiterUtils() {
    //NOP
  }

  /**
   * Attempt to split the rawPeriod into raw periods by delimiter '/' symbol, the symbol must be the only one in the rawPeriod
   *
   * @param rawPeriod raw string date
   *
   * @return always two elements array
   */
  public static String[] splitPeriod(String rawPeriod) {
    //If the delimiter for the first position and last position are the same, symbol only one in string,
    //check length, the length must be greater than seven to avoid case as "1999/2", where it looks as year and month
    boolean canSplit = rawPeriod.lastIndexOf(CHAR_PERIOD) == rawPeriod.indexOf(CHAR_PERIOD) && rawPeriod.length() > 7;
    String[] splited = canSplit ? RGX_PERIOD.split(rawPeriod) : new String[] {rawPeriod, ""};
    //Returns an array of the same length each time
    return splited.length < 2 ? new String[] {splited[0], ""} : splited;
  }

  public static String[] splitDateTime(String rawDateTime) {
    //Does value have time inside
    int timeDelimiterIdx = rawDateTime.indexOf(TIME_DELIMITER);
    boolean hasTime = timeDelimiterIdx > 0;

    //Calculate a shift index of the time, for case as 09:00 or 9:00
    int shift = -1;
    for (int x = timeDelimiterIdx - 1; x > 0; x--) {
      char sign = rawDateTime.charAt(x);
      if (!Character.isDigit(sign)) {
        shift = x;
        break;
      }
    }

    //Calculate the beginning index of date, the date can be absent, in this case, substring from 0 to 0 returns empty string
    int dateIdx = shift < 0 ? 0 : shift;
    String date = hasTime ? rawDateTime.substring(0, dateIdx) : rawDateTime;

    //Calculate the beginning index of time
    int timeIdx = shift + 1;
    String time = hasTime ? rawDateTime.substring(timeIdx) : "";

    return new String[] {date, time};
  }

  public static String[] splitDate(String rawDate) {
    String filtered = RGX_FILTER.matcher(rawDate).replaceAll(CHAR_REPLACE).trim();
    return RGX_REPLACE_CHAR.split(filtered);
  }

  public static String[] splitTime(String rawTime) {
    String minus = RGX_MINUS.matcher(rawTime).replaceAll(MINUS);
    String plus = RGX_PLUS.matcher(minus).replaceAll(PLUS);
    String z = RGX_Z.matcher(plus).replaceAll(Z);

    String[] timeZoneArray = RGX_ESCAPE.split(z.trim());
    return RGX_TIME.split(timeZoneArray[0]);
  }
}
