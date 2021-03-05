package org.gbif.pipelines.core.parsers.vertnet;

import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Parser;

import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This is a copy of VerNet python parser/extractor made in Java:
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/total_length_parser.py
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LengthParser {

  private static final String TOTAL_LENGTH = "total length";
  private static final String HEAD_BODY_LENGTH = "head-body length";
  private static final String FORK_LENGTH = "fork length";
  private static final String STANDARD_LENGTH = "standard length";
  private static final String SNOUT_VENT_LENGTH = "snout-vent length";
  private static final String SHORTHAND = "_shorthand_";
  private static final String ENGLISH = "_english_";

  private static final DynamicParser PARSER =
      DynamicParser.builder()
          .parserName(Parser.LENGTH)

          // Add all replacement values for regex <key> group
          .replaceRegexKeyGroupResultWith(ENGLISH, TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith(SHORTHAND, TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("body", HEAD_BODY_LENGTH)
          .replaceRegexKeyGroupResultWith("body length", HEAD_BODY_LENGTH)
          .replaceRegexKeyGroupResultWith("catalog", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith(FORK_LENGTH, FORK_LENGTH)
          .replaceRegexKeyGroupResultWith("forklength", FORK_LENGTH)
          .replaceRegexKeyGroupResultWith("headbodylengthinmillimeters", HEAD_BODY_LENGTH)
          .replaceRegexKeyGroupResultWith("label length", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("label. length", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("label.length", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("length", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("lengthinmillimeters", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("lengths", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("max length", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("maxlength", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("mean length", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("meas", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("meas,", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("meas.", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("meas. h.b.", HEAD_BODY_LENGTH)
          .replaceRegexKeyGroupResultWith("meas: l", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurement", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements are", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements made", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements of", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements questionable", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements read", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements reads", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurements: l", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("measurementsnt", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("mesurements", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("on tag", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("s.l", STANDARD_LENGTH)
          .replaceRegexKeyGroupResultWith("s.l.", STANDARD_LENGTH)
          .replaceRegexKeyGroupResultWith("s.v.", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("sl", STANDARD_LENGTH)
          .replaceRegexKeyGroupResultWith("sl.", STANDARD_LENGTH)
          .replaceRegexKeyGroupResultWith("snout vent length", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("snout vent lengths", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith(SNOUT_VENT_LENGTH, SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("snout-vent lengths", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("snoutventlengthinmm", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("specimen", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("specimens", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith(STANDARD_LENGTH, STANDARD_LENGTH)
          .replaceRegexKeyGroupResultWith("sv", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("svl", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("svl.", SNOUT_VENT_LENGTH)
          .replaceRegexKeyGroupResultWith("t.l", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("t.l.", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("tag", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("tl", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("tl.", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("tl_", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("tol", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("total", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("total  length", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith(TOTAL_LENGTH, TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("total length in mm", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("total lengths", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("totallength", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("totallengthin", TOTAL_LENGTH)
          .replaceRegexKeyGroupResultWith("totallengthinmm", TOTAL_LENGTH)

          // In the case when key may contain units
          .addUnitsFromKeyRegex("(?<units>mm|millimeters)$")

          // Add templates for common regex functions

          // Used for parsing forms like: 2 ft 4 inches
          .addRegexTemplate("(?&len_foot)", "(?:foot|feet|ft)s?(?&dot)")
          .addRegexTemplate("(?&len_inch)", "(?:inche?|in)s?(?&dot)")
          // Abbreviations for total length
          .addRegexTemplate("(?&len_key_abbrev)", "t(?&dot)o?l(?&dot)|s(?&dot)l(?&dot)")
          // For when the key is a suffix like: 44 mm TL
          .addRegexTemplate("(?&len_key_suffix)", "(?:in\\s*)?(?&len_key_abbrev)")
          // Ambiguous length keys
          .addRegexTemplate("(?&len_key_ambiguous)", "lengths?|tag")
          // Length keys found in phrases
          .addRegexTemplate(
              "(?&len_in_phrase)",
              "(?:total\\s+length|snout\\s+vent\\s+length|standard\\s+length)s?")
          // Other keys that may be used as a proxy for total length for some groups
          .addRegexTemplate(
              "(?&other_len_key)",
              "head(?&dash)body(?&dash)length(?&dash)in(?&dash)millimeters|(?:fork|mean|body)(?&dash)lengths?|t[o.]?l(?&dot)_?")
          // Look for a shorthand total length. Make sure this isn't a date
          .addRegexTemplate(
              "(?&len_shorthand)",
              "(?&dash_req)(?:(?&number)|(?&shorthand_unknown))(?:(?&shorthand_sep)(?:(?&number)|(?&shorthand_unknown))){2,}")
          // The "European" version of the shorthand length
          .addRegexTemplate(
              "(?&len_shorthand_euro)",
              "(?&dash_req)(?:(?&number)|(?&shorthand_unknown))(?:(?&shorthand_sep)(?:(?<![\\w\\-])(?&number)|(?&shorthand_unknown))[a-zA-Z]{0,3}){2,}")
          // Keys that indicate we have a total length
          .addRegexTemplate(
              "(?&total_len_key)",
              "total(?&dash)length(?&dash)in(?&dash)mm|total(?&dash)length(?&dash)in|length(?&dash)in(?&dash)millimeters|(?:total|max|standard)(?&dash)lengths?|meas(?:[a-zA-Z]*)?(?&dot):\\s*l|s(?&dot)l(?&dot)|label(?&dot)\\s*lengths?")
          // Snout-vent length is sometimes used as a proxy for total length in some groups
          .addRegexTemplate(
              "(?&svl_len_key)",
              "snout(?&dash)vent(?&dash)lengths?(?:(?&dash)in(?&dash)mm)?|s(?&dot)v(?&dot)(:?l(?&dot))?|snout\\s+vent\\s+lengths?")
          // Length unit words
          .addRegexTemplate(
              "(?&len_units_word)", "(?:meter|millimeter|centimeter|foot|feet|inche?)s?")
          // Length unit abbreviations
          .addRegexTemplate("(?&len_units_abbrev)", "(?:[cm](?&dot)m|in|ft)(?&dot)")
          // All length units
          .addRegexTemplate("(?&len_units)", "(?&len_units_word)|(?&len_units_abbrev)")
          // Gather all length key types
          .addRegexTemplate(
              "(?&all_len_keys)",
              "(?&total_len_key)|(?&svl_len_key)|(?&other_len_key)|(?&len_key_ambiguous)|(?&key_units_req)|(?&shorthand_words)|(?&shorthand_typos)")

          // Add template matches, which will be applied to values

          // Look for a pattern like: total length: 4 ft 8 in
          .addMatchPattern(
              "\\b(?<key>(?&all_len_keys))?(?&key_end)?"
                  + "(?<value1>(?&range))\\s*"
                  + "(?<units1>(?&len_foot))\\s*"
                  + "(?<value2>(?&range))\\s*"
                  + "(?<units2>(?&len_inch))",
              ENGLISH,
              null)
          // Look for total key, number (not a range) and optional units
          // Like: total length = 10.5 mm
          .addMatchPattern(
              "\\b(?<key>(?&total_len_key))(?&key_end)"
                  + "(?<value>(?&number))(?![\\d\\-\\.])\\s*"
                  + "(?<units>(?&len_units))?",
              null,
              "mm")
          // Look for these secondary length keys next but allow a range
          .addMatchPattern(
              "\\b(?<key>(?&other_len_key))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&len_units))?")
          // Look for keys where the units are required
          .addMatchPattern(
              "\\b(?<key>(?&key_units_req))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&len_units))")
          // Look for a length in a phrase
          .addMatchPattern(
              "\\b(?<key>(?&len_in_phrase))\\D{1,32}"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&len_units))?")
          // These ambiguous keys have a suffix that disambiguate them
          .addMatchPattern(
              "(?&no_word)(?&len_key_ambiguous)(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&len_units))?\\s*"
                  + "(?<key>(?&len_key_suffix))")
          // These keys require units to disambiguate what is being measured
          .addMatchPattern(
              "(?&no_word)"
                  + "(?<key>(?&len_key_ambiguous))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&len_units))")
          // An out of order parse: tol (mm) 20-25
          .addMatchPattern(
              "\\b(?<key>(?&len_key_abbrev))\\s*"
                  + "(?&open)\\s*(?<units>(?&len_units))\\s*(?&close)\\s*"
                  + "(?<value>(?&range))")
          // This parse addKeyValues the key at the end: 20-25 mm TL
          .addMatchPattern(
              "\\b(?<value>(?&range))\\s*(?<units>(?&len_units))?\\s*(?<key>(?&len_key_suffix))")
          // Length is in shorthand notation
          .addMatchPattern(
              "\\b(?:(?<key>(?&all_len_keys))(?&key_end))?(?<value>(?&number))(?&len_shorthand)",
              SHORTHAND,
              "mm")
          // A shorthand notation with some abbreviations in it
          .addMatchPattern(
              "\\b(?:(?<key>(?&all_len_keys))(?&key_end))?"
                  + "[a-z]*"
                  + "(?<value>(?&number))"
                  + "(?&len_shorthand_euro)",
              SHORTHAND,
              "mm")
          // Now we can look for the total length, RANGE, optional units
          // See 'total_len_key_num' above
          .addMatchPattern(
              "\\b(?<key>(?&total_len_key))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&len_units))?",
              null,
              "mm")
          // We will now allow an ambiguous key if it is not preceded by another word
          .addMatchPattern("(?&no_word)(?<key>(?&len_key_ambiguous))(?&key_end)(?<value>(?&range))")
          // Look for snout-vent length keys
          .addMatchPattern(
              "\\b(?<key>(?&svl_len_key))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&len_units))?")
          .build();

  public static Optional<DynamicProperty> parse(String source) {
    return PARSER.parse(source);
  }
}
