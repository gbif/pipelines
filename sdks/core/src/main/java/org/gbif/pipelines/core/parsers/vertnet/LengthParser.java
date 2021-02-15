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

  private static final DynamicParser PARSER =
      DynamicParser.builder()
          .parserName(Parser.LENGTH)

          // Add all replacement values for regex <key> group
          .replaceRegexKeyGroupResultWith("_english_", "total length")
          .replaceRegexKeyGroupResultWith("_shorthand_", "total length")
          .replaceRegexKeyGroupResultWith("body", "head-body length")
          .replaceRegexKeyGroupResultWith("body length", "head-body length")
          .replaceRegexKeyGroupResultWith("catalog", "total length")
          .replaceRegexKeyGroupResultWith("fork length", "fork length")
          .replaceRegexKeyGroupResultWith("forklength", "fork length")
          .replaceRegexKeyGroupResultWith("headbodylengthinmillimeters", "head-body length")
          .replaceRegexKeyGroupResultWith("label length", "total length")
          .replaceRegexKeyGroupResultWith("label. length", "total length")
          .replaceRegexKeyGroupResultWith("label.length", "total length")
          .replaceRegexKeyGroupResultWith("length", "total length")
          .replaceRegexKeyGroupResultWith("lengthinmillimeters", "total length")
          .replaceRegexKeyGroupResultWith("lengths", "total length")
          .replaceRegexKeyGroupResultWith("max length", "total length")
          .replaceRegexKeyGroupResultWith("maxlength", "total length")
          .replaceRegexKeyGroupResultWith("mean length", "total length")
          .replaceRegexKeyGroupResultWith("meas", "total length")
          .replaceRegexKeyGroupResultWith("meas,", "total length")
          .replaceRegexKeyGroupResultWith("meas.", "total length")
          .replaceRegexKeyGroupResultWith("meas. h.b.", "head-body length")
          .replaceRegexKeyGroupResultWith("meas: l", "total length")
          .replaceRegexKeyGroupResultWith("measurement", "total length")
          .replaceRegexKeyGroupResultWith("measurements", "total length")
          .replaceRegexKeyGroupResultWith("measurements are", "total length")
          .replaceRegexKeyGroupResultWith("measurements made", "total length")
          .replaceRegexKeyGroupResultWith("measurements of", "total length")
          .replaceRegexKeyGroupResultWith("measurements questionable", "total length")
          .replaceRegexKeyGroupResultWith("measurements read", "total length")
          .replaceRegexKeyGroupResultWith("measurements reads", "total length")
          .replaceRegexKeyGroupResultWith("measurements: l", "total length")
          .replaceRegexKeyGroupResultWith("measurementsnt", "total length")
          .replaceRegexKeyGroupResultWith("mesurements", "total length")
          .replaceRegexKeyGroupResultWith("on tag", "total length")
          .replaceRegexKeyGroupResultWith("s.l", "standard length")
          .replaceRegexKeyGroupResultWith("s.l.", "standard length")
          .replaceRegexKeyGroupResultWith("s.v.", "snout-vent length")
          .replaceRegexKeyGroupResultWith("sl", "standard length")
          .replaceRegexKeyGroupResultWith("sl.", "standard length")
          .replaceRegexKeyGroupResultWith("snout vent length", "snout-vent length")
          .replaceRegexKeyGroupResultWith("snout vent lengths", "snout-vent length")
          .replaceRegexKeyGroupResultWith("snout-vent length", "snout-vent length")
          .replaceRegexKeyGroupResultWith("snout-vent lengths", "snout-vent length")
          .replaceRegexKeyGroupResultWith("snoutventlengthinmm", "snout-vent length")
          .replaceRegexKeyGroupResultWith("specimen", "total length")
          .replaceRegexKeyGroupResultWith("specimens", "total length")
          .replaceRegexKeyGroupResultWith("standard length", "standard length")
          .replaceRegexKeyGroupResultWith("sv", "snout-vent length")
          .replaceRegexKeyGroupResultWith("svl", "snout-vent length")
          .replaceRegexKeyGroupResultWith("svl.", "snout-vent length")
          .replaceRegexKeyGroupResultWith("t.l", "total length")
          .replaceRegexKeyGroupResultWith("t.l.", "total length")
          .replaceRegexKeyGroupResultWith("tag", "total length")
          .replaceRegexKeyGroupResultWith("tl", "total length")
          .replaceRegexKeyGroupResultWith("tl.", "total length")
          .replaceRegexKeyGroupResultWith("tl_", "total length")
          .replaceRegexKeyGroupResultWith("tol", "total length")
          .replaceRegexKeyGroupResultWith("total", "total length")
          .replaceRegexKeyGroupResultWith("total  length", "total length")
          .replaceRegexKeyGroupResultWith("total length", "total length")
          .replaceRegexKeyGroupResultWith("total length in mm", "total length")
          .replaceRegexKeyGroupResultWith("total lengths", "total length")
          .replaceRegexKeyGroupResultWith("totallength", "total length")
          .replaceRegexKeyGroupResultWith("totallengthin", "total length")
          .replaceRegexKeyGroupResultWith("totallengthinmm", "total length")

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
              "_english_",
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
              "_shorthand_",
              "mm")
          // A shorthand notation with some abbreviations in it
          .addMatchPattern(
              "\\b(?:(?<key>(?&all_len_keys))(?&key_end))?"
                  + "[a-z]*"
                  + "(?<value>(?&number))"
                  + "(?&len_shorthand_euro)",
              "_shorthand_",
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
