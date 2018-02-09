package org.gbif.pipelines.interpretation.parsers.temporal;

import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Interpreter for raw time. The main method interpret
 */
class InterpretRawTime {

  private static final Pattern RGX_TIME = Pattern.compile("[^\\d]");
  private static final Pattern RGX_MINUS = Pattern.compile("[-]");
  private static final Pattern RGX_PLUS = Pattern.compile("[+]");
  private static final Pattern RGX_Z = Pattern.compile("[zZ]");
  private static final Pattern RGX_ESCAPE = Pattern.compile("m");

  private static final String ESCAPE = "m";
  private static final String MINUS = ESCAPE + "-";
  private static final String PLUS = ESCAPE + "+";
  private static final String Z = ESCAPE + "Z";

  private InterpretRawTime() {
    //NOP
  }

  static ChronoStorage interpret(String rawTime) {
    ChronoStorage chronoStorage = new ChronoStorage();
    if (isEmpty(rawTime)) {
      return chronoStorage;
    }
    //Split by some zone char
    String minus = RGX_MINUS.matcher(rawTime).replaceAll(MINUS);
    String plus = RGX_PLUS.matcher(minus).replaceAll(PLUS);
    String z = RGX_Z.matcher(plus).replaceAll(Z);

    String[] timeZoneArray = RGX_ESCAPE.split(z.trim());
    String[] timeArray = RGX_TIME.split(timeZoneArray[0]);

    //Parse time only
    if (timeArray.length > 1) {
      chronoStorage.convertAndPut(HOUR_OF_DAY, timeArray[0]);
      chronoStorage.convertAndPut(MINUTE_OF_HOUR, timeArray[1]);
      if (timeArray.length > 2) {
        chronoStorage.convertAndPut(SECOND_OF_MINUTE, timeArray[2]);
      }
    }

    return chronoStorage;
  }

}

