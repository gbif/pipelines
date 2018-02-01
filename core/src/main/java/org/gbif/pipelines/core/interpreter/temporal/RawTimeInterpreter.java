package org.gbif.pipelines.core.interpreter.temporal;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Interpreter for raw time. The main method interpret
 */
public class RawTimeInterpreter {

  private static final String RGX_TIME = "[^\\d]";
  private static final String ESCAPE = "m";

  private RawTimeInterpreter() {
    //NOP
  }

  public static ParsedDate interpret(ParsedDate pDate, String rTime) {
    ParsedDate parsedDate = ParsedDate.copy(pDate);
    if (isEmpty(rTime)) {
      return parsedDate;
    }
    //Split by some zone char
    String[] timeZoneArray = rTime.replaceAll("[-]", ESCAPE + "-")
      .replaceAll("[+]", ESCAPE + "+")
      .replaceAll("[zZ]", ESCAPE + "Z")
      .trim()
      .split(ESCAPE);

    String[] split = timeZoneArray[0].split(RGX_TIME);

    //Parse time only
    if (split.length > 1) {
      parsedDate.parseAndSetHour(split[0]);
      parsedDate.parseAndSetMinute(split[1]);
      if (split.length > 2) {
        parsedDate.parseAndSetSecond(split[2]);
      }
    }

    //Parse time zone
    if (timeZoneArray.length > 1) {
      String zone = timeZoneArray[1];
      if (!isEmpty(zone) && zone.indexOf(' ') != -1) {
        String[] split1 = zone.split(" ");
        if (split1.length > 0) {
          parsedDate.parseAndSetZone(split1[0]);
        }
      } else {
        parsedDate.parseAndSetZone(zone);
      }
    }

    return parsedDate;
  }

}

