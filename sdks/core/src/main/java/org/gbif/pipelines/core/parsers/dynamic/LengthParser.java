package org.gbif.pipelines.core.parsers.dynamic;

import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.DynamicProperty;

/**
 * Java version of
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/total_length_parser.py
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LengthParser {

  private static final Map<String, String> LENGTH_MAP = new HashMap<>();

  static {
    LENGTH_MAP.put("_english_", "total length");
    LENGTH_MAP.put("_shorthand_", "total length");
    LENGTH_MAP.put("body", "head-body length");
    LENGTH_MAP.put("body length", "head-body length");
    LENGTH_MAP.put("catalog", "total length");
    LENGTH_MAP.put("fork length", "fork length");
    LENGTH_MAP.put("forklength", "fork length");
    LENGTH_MAP.put("headbodylengthinmillimeters", "head-body length");
    LENGTH_MAP.put("label length", "total length");
    LENGTH_MAP.put("label. length", "total length");
    LENGTH_MAP.put("label.length", "total length");
    LENGTH_MAP.put("length", "total length");
    LENGTH_MAP.put("lengthinmillimeters", "total length");
    LENGTH_MAP.put("lengths", "total length");
    LENGTH_MAP.put("max length", "total length");
    LENGTH_MAP.put("maxlength", "total length");
    LENGTH_MAP.put("mean length", "total length");
    LENGTH_MAP.put("meas", "total length");
    LENGTH_MAP.put("meas,", "total length");
    LENGTH_MAP.put("meas.", "total length");
    LENGTH_MAP.put("meas. h.b.", "head-body length");
    LENGTH_MAP.put("meas: l", "total length");
    LENGTH_MAP.put("measurement", "total length");
    LENGTH_MAP.put("measurements", "total length");
    LENGTH_MAP.put("measurements are", "total length");
    LENGTH_MAP.put("measurements made", "total length");
    LENGTH_MAP.put("measurements of", "total length");
    LENGTH_MAP.put("measurements questionable", "total length");
    LENGTH_MAP.put("measurements read", "total length");
    LENGTH_MAP.put("measurements reads", "total length");
    LENGTH_MAP.put("measurements: l", "total length");
    LENGTH_MAP.put("measurementsnt", "total length");
    LENGTH_MAP.put("mesurements", "total length");
    LENGTH_MAP.put("on tag", "total length");
    LENGTH_MAP.put("s.l", "standard length");
    LENGTH_MAP.put("s.l.", "standard length");
    LENGTH_MAP.put("s.v.", "snout-vent length");
    LENGTH_MAP.put("sl", "standard length");
    LENGTH_MAP.put("sl.", "standard length");
    LENGTH_MAP.put("snout vent length", "snout-vent length");
    LENGTH_MAP.put("snout vent lengths", "snout-vent length");
    LENGTH_MAP.put("snout-vent length", "snout-vent length");
    LENGTH_MAP.put("snout-vent lengths", "snout-vent length");
    LENGTH_MAP.put("snoutventlengthinmm", "snout-vent length");
    LENGTH_MAP.put("specimen", "total length");
    LENGTH_MAP.put("specimens", "total length");
    LENGTH_MAP.put("standard length", "standard length");
    LENGTH_MAP.put("sv", "snout-vent length");
    LENGTH_MAP.put("svl", "snout-vent length");
    LENGTH_MAP.put("svl.", "snout-vent length");
    LENGTH_MAP.put("t.l", "total length");
    LENGTH_MAP.put("t.l.", "total length");
    LENGTH_MAP.put("tag", "total length");
    LENGTH_MAP.put("tl", "total length");
    LENGTH_MAP.put("tl.", "total length");
    LENGTH_MAP.put("tl_", "total length");
    LENGTH_MAP.put("tol", "total length");
    LENGTH_MAP.put("total", "total length");
    LENGTH_MAP.put("total  length", "total length");
    LENGTH_MAP.put("total length", "total length");
    LENGTH_MAP.put("total length in mm", "total length");
    LENGTH_MAP.put("total lengths", "total length");
    LENGTH_MAP.put("totallength", "total length");
    LENGTH_MAP.put("totallengthin", "total length");
    LENGTH_MAP.put("totallengthinmm", "total length");
  }

  private static final DynamicParser PARSER = DynamicParser.create();

  static {
    // Used for parsing forms like: 2 ft 4 inches
    PARSER.put("(?&len_foot)", "(?:foot|feet|ft)s?(?&dot)");
    PARSER.put("(?&len_inch)", "(?:inch e?|in)s?(?&dot)");
    // Abbreviations for total length
    PARSER.put("(?&len_key_abbrev)", "t(?&dot)o?l(?&dot)|s(?&dot)l(?&dot)");
    // For when the key is a suffix like: 44 mm TL
    PARSER.put("(?&len_key_suffix)", "(?:in\\s*)?(?&len_key_abbrev)");
    // Ambiguous length keys
    PARSER.put("(?&len_key_ambiguous)", "lengths?|tag");
    // Length keys found in phrases
    PARSER.put(
        "(?&len_in_phrase)", "(?:total\\s+length|snout\\s+vent\\s+length|standard\\s+length)s?");
    // Other keys that may be used as a proxy for total length for some groups
    PARSER.put(
        "(?&other_len_key)",
        "head(?&dash)body(?&dash)length(?&dash)in(?&dash)millimeters|(?:fork|mean|body)(?&dash)lengths?|t[o.]?l(?&dot)_?");
    // Look for a shorthand total length. Make sure this isn't a date
    PARSER.put(
        "(?&len_shorthand)",
        "(?&dash_req)(?:(?&number)|(?&shorthand_unknown))(?:(?&shorthand_sep)(?:(?&number)|(?&shorthand_unknown))){2,}");
    // The "European" version of the shorthand length
    PARSER.put(
        "(?&len_shorthand_euro)",
        "(?&dash_req)(?:(?&number)|(?&shorthand_unknown))(?:(?&shorthand_sep)(?:(?<![\\w\\-])(?&number)|(?&shorthand_unknown))[a-zA-Z]{0,3}){2,}");
    // Keys that indicate we have a total length
    PARSER.put(
        "(?&total_len_key)",
        "total(?&dash)length(?&dash)in(?&dash)mm|total(?&dash)length(?&dash)in|length(?&dash)in(?&dash)millimeters|(?:total|max|standard)(?&dash)lengths?|meas(?:[a-z]*)?(?&dot):\\s*L|s(?&dot)l(?&dot)|label(?&dot)\\s*lengths?");
    // Snout-vent length is sometimes used as a proxy for total length in some groups
    PARSER.put(
        "(?&svl_len_key)",
        "snout(?&dash)vent(?&dash)lengths?(?:(?&dash)in(?&dash)mm)?|s(?&dot)v(?&dot)(:?l(?&dot))?|snout\\s+vent\\s+lengths?");
    // Length unit words
    PARSER.put("(?&len_units_word)", "(?:meter|millimeter|centimeter|foot|feet|inch e?)s?");
    // Length unit abbreviations
    PARSER.put("(?&len_units_abbrev)", "(?:[cm](?&dot)m|in|ft)(?&dot)s?");
    // All length units
    PARSER.put("(?&len_units)", "(?&len_units_word)|(?&len_units_abbrev)");
    // Gather all length key types
    PARSER.put(
        "(?&all_len_keys)",
        "(?&total_len_key)|(?&svl_len_key)|(?&other_len_key)|(?&len_key_ambiguous)|(?&key_units_req)|(?&shorthand_words)|(?&shorthand_typos)");

    PARSER.initMap();
  }

  // Look for a pattern like: total length: 4 ft 8 in
  private static final String EN_LEN =
      "\\b(?<key>(?&all_len_keys))?(?&key_end)?"
          + "(?<value1>(?&range))\\s*"
          + "(?<units1>(?&len_foot))\\s*"
          + "(?<value2>(?&range))\\s*"
          + "(?<units2>(?&len_inch))";
  private static final Pattern EN_LEN_PT = PARSER.initPattern(EN_LEN);

  // Look for total key, number (not a range) and optional units
  // Like: total length = 10.5 mm
  private static final String TOTAL_LEN_KEY_NUM =
      "\\b(?<key>(?&total_len_key))(?&key_end)"
          + "(?<value>(?&number))(?![\\d\\-\\.])\\s*"
          + "(?<units>(?&len_units))?";
  private static final Pattern TOTAL_LEN_KEY_NUM_PT = PARSER.initPattern(TOTAL_LEN_KEY_NUM);

  // Look for these secondary length keys next but allow a range
  private static final String OTHER_LEN_KEY =
      "\\b(?<key>(?&other_len_key))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&len_units))?";
  private static final Pattern OTHER_LEN_KEY_PT = PARSER.initPattern(OTHER_LEN_KEY);

  // Look for keys where the units are required
  private static final String KEY_UNITS_REQ =
      "\\b(?<key>(?&key_units_req))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&len_units))";
  private static final Pattern KEY_UNITS_REQ_PT = PARSER.initPattern(KEY_UNITS_REQ);

  // Look for a length in a phrase
  private static final String LEN_IN_PHRASE =
      "\\b(?<key>(?&len_in_phrase))\\D{1,32}"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&len_units))?";
  private static final Pattern LEN_IN_PHRASE_PT = PARSER.initPattern(LEN_IN_PHRASE);

  // These ambiguous keys have a suffix that disambiguate them
  private static final String LEN_KEY_AMBIGUOUS_UNITS =
      "(?&no_word)(?&len_key_ambiguous)(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&len_units))?\\s*"
          + "(?<key>(?&len_key_suffix))";
  private static final Pattern LEN_KEY_AMBIGUOUS_UNITS_PT =
      PARSER.initPattern(LEN_KEY_AMBIGUOUS_UNITS);

  // These keys require units to disambiguate what is being measured
  private static final String LEN_KEY_AMBIGUOUS_UNITS_2 =
      "(?&no_word)"
          + "(?<key>(?&len_key_ambiguous))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&len_units))";
  private static final Pattern LEN_KEY_AMBIGUOUS_UNITS_2_PT =
      PARSER.initPattern(LEN_KEY_AMBIGUOUS_UNITS_2);

  // An out of order parse: tol (mm) 20-25
  private static final String LEN_KEY_ABBREV =
      "\\b(?<key>(?&len_key_abbrev))\\s*"
          + "(?&open)\\s*(?<units>(?&len_units))\\s*(?&close)\\s*"
          + "(?<value>(?&range))";
  private static final Pattern LEN_KEY_ABBREV_PT = PARSER.initPattern(LEN_KEY_ABBREV);

  // This parse puts the key at the end: 20-25 mm TL
  private static final String LEN_KEY_SUFFIX =
      "\\b(?<value>(?&range))\\s*(?<units>(?&len_units))?\\s*(?<key>(?&len_key_suffix))";
  private static final Pattern LEN_KEY_SUFFIX_PT = PARSER.initPattern(LEN_KEY_SUFFIX);

  // Length is in shorthand notation
  private static final String LEN_SHORTHAND =
      "\\b(?:(?<key>(?&all_len_keys))(?&key_end))?(?<value>(?&number))(?&len_shorthand)";
  private static final Pattern LEN_SHORTHAND_PT = PARSER.initPattern(LEN_SHORTHAND);

  // A shorthand notation with some abbreviations in it
  private static final String LEN_SHORTHAND_EURO =
      "\\b(?:(?<key>(?&all_len_keys))(?&key_end))?"
          + "[a-z]*"
          + "(?<value>(?&number))"
          + "(?&len_shorthand_euro)";
  private static final Pattern LEN_SHORTHAND_EURO_PT = PARSER.initPattern(LEN_SHORTHAND_EURO);

  // Now we can look for the total length, RANGE, optional units
  // See 'total_len_key_num' above
  private static final String TOTAL_LEN_KEY =
      "\\b(?<key>(?&total_len_key))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&len_units))?";
  private static final Pattern TOTAL_LEN_KEY_PT = PARSER.initPattern(TOTAL_LEN_KEY);

  // We will now allow an ambiguous key if it is not preceded by another word
  private static final String LEN_KEY_AMBIGUOUS =
      "(?&no_word)(?<key>(?&len_key_ambiguous))(?&key_end)(?<value>(?&range))";
  private static final Pattern LEN_KEY_AMBIGUOUS_PT = PARSER.initPattern(LEN_KEY_AMBIGUOUS);

  // Look for snout-vent length keys
  private static final String SVL_LEN_KEY =
      "\\b(?<key>(?&svl_len_key))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&len_units))?";
  private static final Pattern SVL_LEN_KEY_PT = PARSER.initPattern(SVL_LEN_KEY);

  private static final Pattern UNITS_FROM_KEY = Pattern.compile("(?<units> mm | millimeters ) $");

  private static final List<Pattern> PATTERNS =
      Arrays.asList(
          EN_LEN_PT,
          TOTAL_LEN_KEY_NUM_PT,
          OTHER_LEN_KEY_PT,
          KEY_UNITS_REQ_PT,
          LEN_IN_PHRASE_PT,
          LEN_KEY_AMBIGUOUS_UNITS_PT,
          LEN_KEY_AMBIGUOUS_UNITS_2_PT,
          LEN_KEY_ABBREV_PT,
          LEN_KEY_SUFFIX_PT,
          LEN_SHORTHAND_PT,
          LEN_SHORTHAND_EURO_PT,
          TOTAL_LEN_KEY_PT,
          LEN_KEY_AMBIGUOUS_PT,
          SVL_LEN_KEY_PT,
          EN_LEN_PT,
          TOTAL_LEN_KEY_NUM_PT,
          OTHER_LEN_KEY_PT,
          KEY_UNITS_REQ_PT,
          LEN_IN_PHRASE_PT,
          LEN_KEY_AMBIGUOUS_UNITS_PT,
          LEN_KEY_AMBIGUOUS_UNITS_2_PT,
          LEN_KEY_ABBREV_PT,
          LEN_KEY_SUFFIX_PT,
          LEN_SHORTHAND_PT,
          LEN_SHORTHAND_EURO_PT,
          TOTAL_LEN_KEY_PT,
          LEN_KEY_AMBIGUOUS_PT,
          SVL_LEN_KEY_PT);

  public static Optional<DynamicProperty> parse(String source) {
    if (source == null || source.isEmpty()) {
      return Optional.empty();
    }

    try {
      for (Pattern p : PATTERNS) {

        Matcher matcher = p.matcher(source.toLowerCase());

        if (matcher.find()) {
          String key = PARSER.getGroup(matcher, "", "key");
          String value = PARSER.getGroup(matcher, null, "value", "value1", "value2");
          String units = PARSER.getGroup(matcher, null, "units", "units1", "units2");
          if (key != null && units == null) {
            Matcher km = UNITS_FROM_KEY.matcher(key);
            units = km.find() ? km.group("units") : null;
          }

          if (key != null && value != null && LENGTH_MAP.containsKey(key)) {
            DynamicProperty dynamicProperty =
                DynamicProperty.newBuilder()
                    .setKey(LENGTH_MAP.get(key))
                    .setValue(value)
                    .setClazz(Type.STRING)
                    .setType(units)
                    .build();
            return Optional.of(dynamicProperty);
          }
        }
      }
    } catch (RuntimeException ex) {
      return Optional.empty();
    }

    return Optional.empty();
  }
}
