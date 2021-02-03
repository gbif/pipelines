package org.gbif.pipelines.core.parsers.dynamic;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.io.avro.DynamicProperty;

/**
 * Java version of
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/body_mass_parser.py
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MassParser {

  @VisibleForTesting protected static final String TOTAL_WEIGHT = "total weight";

  private static final Map<String, String> TOTAL_WEIGHT_MAP = new HashMap<>();

  static {
    TOTAL_WEIGHT_MAP.put("_english_", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("_shorthand_", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("body", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("body mass", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("body weight", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("body wt", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("body wt.", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("bodymass", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("catalog", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("dead. weight", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("dead. wt", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("dead. wt.", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("full.weight", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("live weight", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("live wt", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("live wt.", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("mass", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("massingrams", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("meas", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("meas.", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("measurement", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("measurements", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("measurements are", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("measurements questionable", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("measurements read", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("measurements reads", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("mesurements", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("observedweight", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("on tag", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("specimen", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("total", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("total weight", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("total wt", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("total wt.", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("w.t.", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("weighed", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("weighing", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("weighs", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("weight", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("weightingrams", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("weights", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("wt", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("wt.", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("wts", TOTAL_WEIGHT);
    TOTAL_WEIGHT_MAP.put("wts.", TOTAL_WEIGHT);
  }

  private static final DynamicParser PARSER;

  static {
    PARSER = DynamicParser.create(Pattern.compile("(?<units>grams)$"), TOTAL_WEIGHT_MAP);
    // Use to parse forms like: 2 lbs 4 oz.
    PARSER.addTemplate("(?&wt_pound)", "(?:pound|lb)s?(?&dot)");
    PARSER.addTemplate("(?&wt_ounce)", "(?:ounce|oz)s?(?&dot)");
    // Used to indicate that the next measurement in a shorthand notation is total mass
    PARSER.addTemplate("(?&wt_shorthand_sep)", "[=\\s\\-]+");
    // Shorthand notation
    PARSER.addTemplate(
        "(?&wt_shorthand)",
        "(?:(?:(?&number)|(?&shorthand_unknown))(?&shorthand_sep)){3,}(?:(?&number)|(?&shorthand_unknown))(?&wt_shorthand_sep)");
    // Shorthand notation requiring units
    PARSER.addTemplate(
        "(?&wt_shorthand_req)", "(?:(?:(?&number)|(?&shorthand_unknown))(?&shorthand_sep)){4,}");
    // A common shorthand notation
    PARSER.addTemplate(
        "(?&wt_shorthand_euro)",
        "(?:(?&number)|(?&shorthand_unknown))hb(?:(?&shorthand_sep)(?:(?<![\\w\\-])(?&number)|(?&shorthand_unknown))[a-z]*){4,}=");
    // Keywords for total mass
    PARSER.addTemplate(
        "(?&total_wt_key)",
        "weightingrams|massingrams|(?:body|full|observed|total)(?&dot)\\s*(?&wt_key_word)");
    // Keywords often used for total mass
    PARSER.addTemplate("(?&other_wt_key)", "(?:dead|live)(?&dot)\\s*(?&wt_key_word)");
    //  Weight keyword
    PARSER.addTemplate("(?&wt_key_word)", "weights?|weigh(?:s|ed|ing)|mass|w(?&dot)ts?(?&dot)");

    // Look for phrases with the total weight
    PARSER.addTemplate("(?&wt_in_phrase)", "total\\s+(?&wt_key_word)");
    // Mass unit words
    PARSER.addTemplate("(?&wt_units_word)", "(?:gram|milligram|kilogram|pound|ounce)s?");
    // Mass unit abbreviations
    PARSER.addTemplate("(?&wt_units_abbrev)", "(?:m(?&dot)g|k(?&dot)g|g[mr]?|lb|oz)s?(?&dot)");
    // All mass units
    PARSER.addTemplate("(?&wt_units)", "(?&wt_units_word)|(?&wt_units_abbrev)");
    // Gather all weight keys
    PARSER.addTemplate(
        "(?&all_wt_keys)",
        "(?&total_wt_key)|(?&other_wt_key)|(?&wt_key_word)|(?&key_units_req)|(?&shorthand_words)|(?&shorthand_typos)");

    PARSER.initTemplates();

    // Look for a pattern like: body mass: 4 lbs 8 oz
    String enWt =
        "\\b(?<key>(?&all_wt_keys))?(?&key_end)?"
            + "(?<value1>(?&range))\\s*"
            + "(?<units1>(?&wt_pound))\\s*"
            + "(?<value2>(?&range))\\s*"
            + "(?<units2>(?&wt_ounce))";
    PARSER.initPattern(enWt, "_english_", null);

    // Look for body mass with a total weight key and optional units
    String totalWtKey =
        "\\b(?<key>(?&total_wt_key))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&wt_units))?";
    PARSER.initPattern(totalWtKey);

    // Look for these secondary body mass keys next
    String otherWtKey =
        "\\b(?<key>(?&other_wt_key))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&wt_units))?";
    PARSER.initPattern(otherWtKey);

    // Look for keys where the units are required
    String keyUnitsReq =
        "\\b(?<key>(?&key_units_req))(?&key_end)"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&wt_units))";
    PARSER.initPattern(keyUnitsReq);

    // Look for the body mass in a phrase
    String wtInPhrase =
        "\\b(?<key>(?&wt_in_phrase))\\D{1,32}"
            + "(?<value>(?&range))\\s*"
            + "(?<units>(?&wt_units))?";
    PARSER.initPattern(wtInPhrase);

    // An out of order parse: body mass (g) 20-25
    String wtKeyWord =
        "\\b(?<key>(?&wt_key_word))\\s*"
            + "(?&open)\\s*(?<units>(?&wt_units))\\s*(?&close)\\s*"
            + "(?<value>(?&range))";
    PARSER.initPattern(wtKeyWord);

    // These keys require units to disambiguate what is being measured
    String wtKeyWordReq =
        "(?<key>(?&wt_key_word))(?&key_end)(?<value>(?&range))\\s*(?<units>(?&wt_units))";
    PARSER.initPattern(wtKeyWordReq);

    // Body mass is in shorthand notation
    String wtShorthand =
        "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
            + "(?&wt_shorthand)\\s*"
            + "(?<value>(?&number))\\s*"
            + "(?<units>(?&wt_units))?";
    PARSER.initPattern(wtShorthand, "_shorthand_", null);

    // Body mass is in shorthand notation (units required)
    String wtShorthandReq =
        "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
            + "(?&wt_shorthand_req)\\s*"
            + "(?<value>(?&number))\\s*"
            + "(?<units>(?&wt_units))";
    PARSER.initPattern(wtShorthandReq, "_shorthand_", null);

    // A shorthand notation with some abbreviations in it
    String wtShorthandEuro =
        "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
            + "(?&wt_shorthand_euro)\\s*"
            + "(?<value>(?&number))\\s*"
            + "(?<units>(?&wt_units))?";
    PARSER.initPattern(wtShorthandEuro, "_shorthand_", null);

    // A notation using 'fa'. It can be shorter than the other shorthand notations
    String wtFa = "fa\\d*-(?<value>(?&number))\\s*(?<units>(?&wt_units))?";
    PARSER.initPattern(wtFa, "_shorthand_", null);

    // Now we can look for the body mass, RANGE, optional units
    String wtKeyAmbiguous =
        "(?<key>(?&wt_key_word))(?&key_end)(?<value>(?&range))\\s*(?<units>(?&wt_units))?";
    PARSER.initPattern(wtKeyAmbiguous);
  }

  public static Optional<DynamicProperty> parse(String source) {
    return PARSER.parse(source);
  }
}
