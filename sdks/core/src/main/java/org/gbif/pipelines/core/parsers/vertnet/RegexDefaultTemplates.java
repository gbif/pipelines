package org.gbif.pipelines.core.parsers.vertnet;

import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Java version of
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/trait_parser.py
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class RegexDefaultTemplates {

  private static final Map<String, String> MAP = new HashMap<>(14);

  static {
    // For our purposes numbers are always positive and decimals.
    MAP.put(
        "(?&number)",
        "[\\(\\[\\{]?(?:\\d{1,3}(?:,\\d{3}){1,3}|\\d+)(?:\\.\\d+)?[\\)\\]\\}]?[\\*]?");
    // We also want to pull in number ranges when appropriate.
    MAP.put(
        "(?&range)",
        "[\\(\\[\\{]?(?:\\d{1,3}(?:,\\d{3}){1,3}|\\d+)(?:\\.\\d+)?[\\)\\]\\}]?[\\*]?(?:\\s*(?:-|to)\\s*[\\(\\[\\{]?(?:\\d{1,3}(?:,\\d{3}){1,3}|\\d+)(?:\\.\\d+)?[\\)\\]\\}]?[\\*]?)?");
    // Characters that follow a keyword
    MAP.put("(?&key_end)", "\\s*[^\\w.\\[\\(]*\\s*");
    // We sometimes want to guarantee no word precedes another word.
    // This cannot be done with negative look behind,
    // so we do a positive search for a separator
    MAP.put("(?&no_word)", "(?:^|[;,:\"'\\{\\[\\(]+)\\s*");
    // Keywords that may precede a shorthand measurement
    MAP.put(
        "(?&shorthand_words)",
        "on\\s*tag|specimens?|catalog|measurements(?:\\s+[a-zA-Z]+)|tag\\s+\\d+\\s*=?(?:male|female)?\\s*,|meas[.,]?(?:\\s+\\w+\\.\\w+\\.)?");
    // Common keyword misspellings that precede shorthand measurement
    MAP.put("(?&shorthand_typos)", "mesurements|measurementsnt");
    // Keys where we need units to know if it's for mass or length
    MAP.put("(?&key_units_req)", "measurements?|body|total");
    // Characters that separate shorthand values
    MAP.put("(?&shorthand_sep)", "[:\\/\\-\\s]");
    // Used in shorthand notation for unknown values
    MAP.put("(?&shorthand_unknown)", "[\\?x]");
    // Look for an optional dash or space character
    MAP.put("(?&dash)", "[\\s\\-]?");
    MAP.put("(?&dash_req)", "[\\s\\-]");
    // Look for an optional dot character
    MAP.put("(?&dot)", "\\.?");
    // Numbers are sometimes surrounded by brackets or parentheses
    // Don't worry about matching the opening and closing brackets
    MAP.put("(?&open)", "[\\(\\[\\{]?");
    MAP.put("(?&close)", "[\\)\\]\\}]?");
  }

  static Map<String, String> get() {
    return MAP;
  }
}
