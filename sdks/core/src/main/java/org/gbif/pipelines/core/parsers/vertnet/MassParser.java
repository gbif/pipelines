package org.gbif.pipelines.core.parsers.vertnet;

import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Parser;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a copy of VerNet python parser/extractor made in Java
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/body_mass_parser.py
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MassParser {

  private static final String ENGLISH = "_english_";
  private static final String SHORTHAND = "_shorthand_";

  @VisibleForTesting protected static final String TOTAL_WEIGHT = "total weight";

  private static final DynamicParser PARSER =
      DynamicParser.builder()
          .parserName(Parser.MASS)

          // Add all replacement values for regex <key> group
          .replaceRegexKeyGroupResultWith(ENGLISH, TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith(SHORTHAND, TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("body", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("body mass", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("body weight", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("body wt", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("body wt.", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("bodymass", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("catalog", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("dead. weight", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("dead. wt", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("dead. wt.", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("full.weight", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("live weight", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("live wt", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("live wt.", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("mass", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("massingrams", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("meas", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("meas.", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("measurement", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("measurements", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("measurements are", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("measurements questionable", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("measurements read", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("measurements reads", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("mesurements", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("observedweight", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("on tag", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("specimen", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("total", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("total weight", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("total wt", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("total wt.", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("w.t.", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("weighed", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("weighing", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("weighs", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("weight", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("weightingrams", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("weights", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("wt", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("wt.", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("wts", TOTAL_WEIGHT)
          .replaceRegexKeyGroupResultWith("wts.", TOTAL_WEIGHT)

          // In the case when key may contain units
          .addUnitsFromKeyRegex("(?<units>grams)$")

          // Add templates for common regex functions

          // Use to parse forms like: 2 lbs 4 oz.
          .addRegexTemplate("(?&wt_pound)", "(?:pound|lb)s?(?&dot)")
          .addRegexTemplate("(?&wt_ounce)", "(?:ounce|oz)s?(?&dot)")
          // Used to indicate that the next measurement in a shorthand notation is total mass
          .addRegexTemplate("(?&wt_shorthand_sep)", "[=\\s\\-]+")
          // Shorthand notation
          .addRegexTemplate(
              "(?&wt_shorthand)",
              "(?:(?:(?&number)|(?&shorthand_unknown))(?&shorthand_sep)){3,}(?:(?&number)|(?&shorthand_unknown))(?&wt_shorthand_sep)")
          // Shorthand notation requiring units
          .addRegexTemplate(
              "(?&wt_shorthand_req)",
              "(?:(?:(?&number)|(?&shorthand_unknown))(?&shorthand_sep)){4,}")
          // A common shorthand notation
          .addRegexTemplate(
              "(?&wt_shorthand_euro)",
              "(?:(?&number)|(?&shorthand_unknown))hb(?:(?&shorthand_sep)(?:(?<![\\w\\-])(?&number)|(?&shorthand_unknown))[a-z]*){4,}=")
          // Keywords for total mass
          .addRegexTemplate(
              "(?&total_wt_key)",
              "weightingrams|massingrams|(?:body|full|observed|total)(?&dot)\\s*(?&wt_key_word)")
          // Keywords often used for total mass
          .addRegexTemplate("(?&other_wt_key)", "(?:dead|live)(?&dot)\\s*(?&wt_key_word)")
          //  Weight keyword
          .addRegexTemplate("(?&wt_key_word)", "weights?|weigh(?:s|ed|ing)|mass|w(?&dot)ts?(?&dot)")

          // Look for phrases with the total weight
          .addRegexTemplate("(?&wt_in_phrase)", "total\\s+(?&wt_key_word)")
          // Mass unit words
          .addRegexTemplate("(?&wt_units_word)", "(?:gram|milligram|kilogram|pound|ounce)s?")
          // Mass unit abbreviations
          .addRegexTemplate("(?&wt_units_abbrev)", "(?:m(?&dot)g|k(?&dot)g|g[mr]?|lb|oz)s?(?&dot)")
          // All mass units
          .addRegexTemplate("(?&wt_units)", "(?&wt_units_word)|(?&wt_units_abbrev)")

          // Add template matches, which will be applied to values

          // Gather all weight keys
          .addRegexTemplate(
              "(?&all_wt_keys)",
              "(?&total_wt_key)|(?&other_wt_key)|(?&wt_key_word)|(?&key_units_req)|(?&shorthand_words)|(?&shorthand_typos)")
          // Look for a pattern like: body mass: 4 lbs 8 oz
          .addMatchPattern(
              "\\b(?<key>(?&all_wt_keys))?(?&key_end)?"
                  + "(?<value1>(?&range))\\s*"
                  + "(?<units1>(?&wt_pound))\\s*"
                  + "(?<value2>(?&range))\\s*"
                  + "(?<units2>(?&wt_ounce))",
              ENGLISH,
              null)
          // Look for body mass with a total weight key and optional units
          .addMatchPattern(
              "\\b(?<key>(?&total_wt_key))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&wt_units))?")
          // Look for these secondary body mass keys next
          .addMatchPattern(
              "\\b(?<key>(?&other_wt_key))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&wt_units))?")
          // Look for keys where the units are required
          .addMatchPattern(
              "\\b(?<key>(?&key_units_req))(?&key_end)"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&wt_units))")
          // Look for the body mass in a phrase
          .addMatchPattern(
              "\\b(?<key>(?&wt_in_phrase))\\D{1,32}"
                  + "(?<value>(?&range))\\s*"
                  + "(?<units>(?&wt_units))?")
          // An out of order parse: body mass (g) 20-25
          .addMatchPattern(
              "\\b(?<key>(?&wt_key_word))\\s*"
                  + "(?&open)\\s*(?<units>(?&wt_units))\\s*(?&close)\\s*"
                  + "(?<value>(?&range))")
          // These keys require units to disambiguate what is being measured
          .addMatchPattern(
              "(?<key>(?&wt_key_word))(?&key_end)(?<value>(?&range))\\s*(?<units>(?&wt_units))")
          // Body mass is in shorthand notation
          .addMatchPattern(
              "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
                  + "(?&wt_shorthand)\\s*"
                  + "(?<value>(?&number))\\s*"
                  + "(?<units>(?&wt_units))?",
              SHORTHAND,
              null)
          // Body mass is in shorthand notation (units required)
          .addMatchPattern(
              "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
                  + "(?&wt_shorthand_req)\\s*"
                  + "(?<value>(?&number))\\s*"
                  + "(?<units>(?&wt_units))",
              SHORTHAND,
              null)
          // A shorthand notation with some abbreviations in it
          .addMatchPattern(
              "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
                  + "(?&wt_shorthand_euro)\\s*"
                  + "(?<value>(?&number))\\s*"
                  + "(?<units>(?&wt_units))?",
              SHORTHAND,
              null)
          // A notation using 'fa'. It can be shorter than the other shorthand notations
          .addMatchPattern(
              "fa\\d*-(?<value>(?&number))\\s*(?<units>(?&wt_units))?", SHORTHAND, null)
          // Now we can look for the body mass, RANGE, optional units
          .addMatchPattern(
              "(?<key>(?&wt_key_word))(?&key_end)(?<value>(?&range))\\s*(?<units>(?&wt_units))?")
          .build();

  public static Optional<DynamicProperty> parse(String source) {
    return PARSER.parse(source);
  }
}
