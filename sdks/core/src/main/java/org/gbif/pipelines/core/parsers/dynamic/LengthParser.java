package org.gbif.pipelines.core.parsers.dynamic;

import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Field;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

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

  private static final DynamicParser PARSER;

  static {
    PARSER =
        DynamicParser.builder()
            .field(Field.LENGTH)
            .keyMap(LENGTH_MAP)
            .unitsFromKey(Pattern.compile("(?<units>mm|millimeters)$"))
            .build();

    // Used for parsing forms like: 2 ft 4 inches
    PARSER.addTemplate("(?&len_foot)", "(?:foot|feet|ft)s?(?&dot)");
    PARSER.addTemplate("(?&len_inch)", "(?:inche?|in)s?(?&dot)");
    // Abbreviations for total length
    PARSER.addTemplate("(?&len_key_abbrev)", "t(?&dot)o?l(?&dot)|s(?&dot)l(?&dot)");
    // For when the key is a suffix like: 44 mm TL
    PARSER.addTemplate("(?&len_key_suffix)", "(?:in\\s*)?(?&len_key_abbrev)");
    // Ambiguous length keys
    PARSER.addTemplate("(?&len_key_ambiguous)", "lengths?|tag");
    // Length keys found in phrases
    PARSER.addTemplate(
        "(?&len_in_phrase)", "(?:total\\s+length|snout\\s+vent\\s+length|standard\\s+length)s?");
    // Other keys that may be used as a proxy for total length for some groups
    PARSER.addTemplate(
        "(?&other_len_key)",
        "head(?&dash)body(?&dash)length(?&dash)in(?&dash)millimeters|(?:fork|mean|body)(?&dash)lengths?|t[o.]?l(?&dot)_?");
    // Look for a shorthand total length. Make sure this isn't a date
    PARSER.addTemplate(
        "(?&len_shorthand)",
        "(?&dash_req)(?:(?&number)|(?&shorthand_unknown))(?:(?&shorthand_sep)(?:(?&number)|(?&shorthand_unknown))){2,}");
    // The "European" version of the shorthand length
    PARSER.addTemplate(
        "(?&len_shorthand_euro)",
        "(?&dash_req)(?:(?&number)|(?&shorthand_unknown))(?:(?&shorthand_sep)(?:(?<![\\w\\-])(?&number)|(?&shorthand_unknown))[a-zA-Z]{0,3}){2,}");
    // Keys that indicate we have a total length
    PARSER.addTemplate(
        "(?&total_len_key)",
        "total(?&dash)length(?&dash)in(?&dash)mm|total(?&dash)length(?&dash)in|length(?&dash)in(?&dash)millimeters|(?:total|max|standard)(?&dash)lengths?|meas(?:[a-zA-Z]*)?(?&dot):\\s*l|s(?&dot)l(?&dot)|label(?&dot)\\s*lengths?");
    // Snout-vent length is sometimes used as a proxy for total length in some groups
    PARSER.addTemplate(
        "(?&svl_len_key)",
        "snout(?&dash)vent(?&dash)lengths?(?:(?&dash)in(?&dash)mm)?|s(?&dot)v(?&dot)(:?l(?&dot))?|snout\\s+vent\\s+lengths?");
    // Length unit words
    PARSER.addTemplate("(?&len_units_word)", "(?:meter|millimeter|centimeter|foot|feet|inche?)s?");
    // Length unit abbreviations
    PARSER.addTemplate("(?&len_units_abbrev)", "(?:[cm](?&dot)m|in|ft)(?&dot)");
    // All length units
    PARSER.addTemplate("(?&len_units)", "(?&len_units_word)|(?&len_units_abbrev)");
    // Gather all length key types
    PARSER.addTemplate(
        "(?&all_len_keys)",
        "(?&total_len_key)|(?&svl_len_key)|(?&other_len_key)|(?&len_key_ambiguous)|(?&key_units_req)|(?&shorthand_words)|(?&shorthand_typos)");

    PARSER.initTemplates();

    // Look for a pattern like: total length: 4 ft 8 in
    String enLen =
        "\\b(?<key>(?&all_len_keys))?(?&key_end)?"
            + "(?<value1>(?&range))\\s*"
            + "(?<units1>(?&len_foot))\\s*"
            + "(?<value2>(?&range))\\s*"
            + "(?<units2>(?&len_inch))";
    PARSER.initPattern(enLen, "_english_", null);

    // Look for total key, number (not a range) and optional units
    // Like: total length = 10.5 mm
    String totalLenKeyNum =
        "\\b(?<key>(?&total_len_key))(?&key_end)"
            + "(?<value>(?&number))(?![\\d\\-\\.])\\s*"
            + "(?<units>(?&len_units))?";
    PARSER.initPattern(totalLenKeyNum, null, "mm");

    // Look for these secondary length keys next but allow a range
    String otherLenKey =
        "\\b(?<key>(?&other_len_key))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&len_units))?";
    PARSER.initPattern(otherLenKey);

    // Look for keys where the units are required
    String keyUnitsReq =
        "\\b(?<key>(?&key_units_req))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&len_units))";
    PARSER.initPattern(keyUnitsReq);

    // Look for a length in a phrase
    String lenInPhrase =
        "\\b(?<key>(?&len_in_phrase))\\D{1,32}"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&len_units))?";
    PARSER.initPattern(lenInPhrase);

    // These ambiguous keys have a suffix that disambiguate them
    String lenKeyAmbiguousUnits =
        "(?&no_word)(?&len_key_ambiguous)(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&len_units))?\\s*"
            + "(?<key>(?&len_key_suffix))";

    PARSER.initPattern(lenKeyAmbiguousUnits);

    // These keys require units to disambiguate what is being measured
    String lenKeyAmbiguousUnits2 =
        "(?&no_word)"
            + "(?<key>(?&len_key_ambiguous))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&len_units))";
    PARSER.initPattern(lenKeyAmbiguousUnits2);

    // An out of order parse: tol (mm) 20-25
    String lenKeyAbbrev =
        "\\b(?<key>(?&len_key_abbrev))\\s*"
            + "(?&open)\\s*(?<units>(?&len_units))\\s*(?&close)\\s*"
            + "(?<value>(?&range))";
    PARSER.initPattern(lenKeyAbbrev);

    // This parse puts the key at the end: 20-25 mm TL
    String lenKeySuffix =
        "\\b(?<value>(?&range))\\s*(?<units>(?&len_units))?\\s*(?<key>(?&len_key_suffix))";
    PARSER.initPattern(lenKeySuffix);

    // Length is in shorthand notation
    String lenShorthand =
        "\\b(?:(?<key>(?&all_len_keys))(?&key_end))?(?<value>(?&number))(?&len_shorthand)";
    PARSER.initPattern(lenShorthand, "_shorthand_", "mm");

    // A shorthand notation with some abbreviations in it
    String lenShorthandEuro =
        "\\b(?:(?<key>(?&all_len_keys))(?&key_end))?"
            + "[a-z]*"
            + "(?<value>(?&number))"
            + "(?&len_shorthand_euro)";
    PARSER.initPattern(lenShorthandEuro, "_shorthand_", "mm");

    // Now we can look for the total length, RANGE, optional units
    // See 'total_len_key_num' above
    String totalLenKey =
        "\\b(?<key>(?&total_len_key))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&len_units))?";
    PARSER.initPattern(totalLenKey, null, "mm");

    // We will now allow an ambiguous key if it is not preceded by another word
    String lenKeyAmbiguous =
        "(?&no_word)(?<key>(?&len_key_ambiguous))(?&key_end)(?<value>(?&range))";
    PARSER.initPattern(lenKeyAmbiguous);

    // Look for snout-vent length keys
    String svlLenKey =
        "\\b(?<key>(?&svl_len_key))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&len_units))?";
    PARSER.initPattern(svlLenKey);
  }

  public static Optional<DynamicProperty> parse(String source) {
    return PARSER.parse(source);
  }
}
