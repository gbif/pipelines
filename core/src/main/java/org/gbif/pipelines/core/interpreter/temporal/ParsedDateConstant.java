package org.gbif.pipelines.core.interpreter.temporal;

import java.time.ZoneId;

class ParsedDateConstant {

  private ParsedDateConstant() {}

  enum ParsedElementEnum {
    NONE, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, ZONE
  }

  static final Integer ISSUE = -1;
  static final ZoneId BASE_ZONE_ID = ZoneId.of("Z");

}
