package org.gbif.pipelines.core.utils;

import org.apache.commons.lang3.StringUtils;

public class StringUtil {

  private StringUtil() {
  }

  public static String cleanName(String x) {
    x = StringUtils.normalizeSpace(x).trim();
    // if we get all upper names, Capitalize them
    if (StringUtils.isAllUpperCase(StringUtils.deleteWhitespace(x))) {
      x = StringUtils.capitalize(x.toLowerCase());
    }
    return x;
  }

}
