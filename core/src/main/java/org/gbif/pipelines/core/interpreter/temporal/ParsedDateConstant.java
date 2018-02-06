package org.gbif.pipelines.core.interpreter.temporal;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ParsedDateConstant {

  private ParsedDateConstant() {}

  enum ParsedElementEnum {
    YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, ZONE
  }

  public static final Integer ISSUE = -1;
  public static final ZoneId BASE_ZONE_ID = ZoneId.of("Z");
  public static final Integer MIN_YEAR = 1000;
  public static final ZonedDateTime BASE_DATE = LocalDateTime.of(1, 1, 1, 0, 0).atZone(BASE_ZONE_ID);

}
