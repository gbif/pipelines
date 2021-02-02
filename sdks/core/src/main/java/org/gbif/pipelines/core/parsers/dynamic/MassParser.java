package org.gbif.pipelines.core.parsers.dynamic;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;
import org.gbif.pipelines.io.avro.DynamicProperty;

/**
 * Java version of
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/body_mass_parser.py
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MassParser {

  private static final Set<String> TOTAL_WEIGHT_SET =
      new HashSet<>(
          Arrays.asList(
              "_english_",
              "_shorthand_",
              "body",
              "body mass",
              "body weight",
              "body wt",
              "body wt.",
              "bodymass",
              "catalog",
              "dead. weight",
              "dead. wt",
              "dead. wt.",
              "full.weight",
              "live weight",
              "live wt",
              "live wt.",
              "mass",
              "massingrams",
              "meas",
              "meas.",
              "measurement",
              "measurements",
              "measurements are",
              "measurements questionable",
              "measurements read",
              "measurements reads",
              "mesurements",
              "observedweight",
              "on tag",
              "specimen",
              "total",
              "total weight",
              "total wt",
              "total wt.",
              "w.t.",
              "weighed",
              "weighing",
              "weighs",
              "weight",
              "weightingrams",
              "weights",
              "wt",
              "wt.",
              "wts",
              "wts."));

  @VisibleForTesting protected static final Map<String, String> R_MAP = new LinkedHashMap<>();

  static {
    // Use to parse forms like: 2 lbs 4 oz.
    put("(?&wt_pound)", "(?:pound|lb)s?(?&dot)");
    put("(?&wt_ounce)", "(?:ounce|oz)s?(?&dot)");
    // Used to indicate that the next measurement in a shorthand notation is total mass
    put("(?&wt_shorthand_sep)", "[=\\s\\-]+");
    // Shorthand notation
    put(
        "(?&wt_shorthand)",
        "(?:(?:(?&number)|(?&shorthand_unknown))(?&shorthand_sep)){3,}(?:(?&number)|(?&shorthand_unknown))(?&wt_shorthand_sep)");
    // Shorthand notation requiring units
    put("(?&wt_shorthand_req)", "(?:(?:(?&number)|(?&shorthand_unknown))(?&shorthand_sep)){4,}");
    // A common shorthand notation
    put(
        "(?&wt_shorthand_euro)",
        "(?:(?&number)|(?&shorthand_unknown))hb(?:(?&shorthand_sep)(?:(?<![\\w\\-])(?&number)|(?&shorthand_unknown))[a-z]*){4,}=");
    // Keywords for total mass
    put(
        "(?&total_wt_key)",
        "weightingrams|massingrams|(?:body|full|observed|total)(?&dot)\\s*(?&wt_key_word)");
    // Keywords often used for total mass
    put("(?&other_wt_key)", "(?:dead|live)(?&dot)\\s*(?&wt_key_word)");
    //  Weight keyword
    put("(?&wt_key_word)", "weights?|weigh(?:s|ed|ing)|mass|w(?&dot)ts?(?&dot)");

    // Look for phrases with the total weight
    put("(?&wt_in_phrase)", "total\\s+(?&wt_key_word)");
    // Mass unit words
    put("(?&wt_units_word)", "(?:gram|milligram|kilogram|pound|ounce)s?");
    // Mass unit abbreviations
    put("(?&wt_units_abbrev)", "(?:m(?&dot)g|k(?&dot)g|g[mr]?|lb|oz)s?(?&dot)");
    // All mass units
    put("(?&wt_units)", "(?&wt_units_word)|(?&wt_units_abbrev)");
    // Gather all weight keys
    put(
        "(?&all_wt_keys)",
        "(?&total_wt_key)|(?&other_wt_key)|(?&wt_key_word)|(?&key_units_req)|(?&shorthand_words)|(?&shorthand_typos)");

    for (Map.Entry<String, String> e : R_MAP.entrySet()) {
      String v = e.getValue();
      for (Map.Entry<String, String> e2 : R_MAP.entrySet()) {
        if (!e.getKey().equals(e2.getKey())) {
          v = v.replace(e2.getKey(), e2.getValue());
        }
      }
      R_MAP.replace(e.getKey(), v);
    }
  }

  // Look for a pattern like: body mass: 4 lbs 8 oz
  private static final String EN_WT =
      "\\b(?<key>(?&all_wt_keys))?(?&key_end)?"
          + "(?<value1>(?&range))\\s*"
          + "(?<units1>(?&wt_pound))\\s*"
          + "(?<value2>(?&range))\\s*"
          + "(?<units2>(?&wt_ounce))";

  private static final Pattern EN_WT_PT = initPattern(EN_WT);

  // Look for body mass with a total weight key and optional units
  private static final String TOTAL_WT_KEY =
      "\\b(?<key>(?&total_wt_key))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&wt_units))?";

  private static final Pattern TOTAL_WT_KEY_PT = initPattern(TOTAL_WT_KEY);

  // Look for these secondary body mass keys next
  private static final String OTHER_WT_KEY =
      "\\b(?<key>(?&other_wt_key))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&wt_units))?";

  private static final Pattern OTHER_WT_KEY_PT = initPattern(OTHER_WT_KEY);

  // Look for keys where the units are required
  private static final String KEY_UNITS_REQ =
      "\\b(?<key>(?&key_units_req))(?&key_end)"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&wt_units))";

  private static final Pattern KEY_UNITS_REQ_PT = initPattern(KEY_UNITS_REQ);

  // Look for the body mass in a phrase
  private static final String WT_IN_PHRASE =
      "\\b(?<key>(?&wt_in_phrase))\\D{1,32}"
          + "(?<value>(?&range))\\s*"
          + "(?<units>(?&wt_units))?";

  private static final Pattern WT_IN_PHRASE_PT = initPattern(WT_IN_PHRASE);

  // An out of order parse: body mass (g) 20-25
  private static final String WT_KEY_WORD =
      "\\b(?<key>(?&wt_key_word))\\s*"
          + "(?&open)\\s*(?<units>(?&wt_units))\\s*(?&close)\\s*"
          + "(?<value>(?&range))";

  private static final Pattern WT_KEY_WORD_PT = initPattern(WT_KEY_WORD);

  // These keys require units to disambiguate what is being measured
  private static final String WT_KEY_WORD_REQ =
      "(?<key>(?&wt_key_word))(?&key_end)(?<value>(?&range))\\s*(?<units>(?&wt_units))";

  private static final Pattern WT_KEY_WORD_REQ_PT = initPattern(WT_KEY_WORD_REQ);

  // Body mass is in shorthand notation
  private static final String WT_SHORTHAND =
      "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
          + "(?&wt_shorthand)\\s*"
          + "(?<value>(?&number))\\s*"
          + "(?<units>(?&wt_units))?";

  private static final Pattern WT_SHORTHAND_PT = initPattern(WT_SHORTHAND);

  // Body mass is in shorthand notation (units required)
  private static final String WT_SHORTHAND_REQ =
      "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
          + "(?&wt_shorthand_req)\\s*"
          + "(?<value>(?&number))\\s*"
          + "(?<units>(?&wt_units))";

  private static final Pattern WT_SHORTHAND_REQ_PT = initPattern(WT_SHORTHAND_REQ);

  // A shorthand notation with some abbreviations in it
  private static final String WT_SHORTHAND_EURO =
      "\\b(?:(?<key>(?&all_wt_keys))(?&key_end))?"
          + "(?&wt_shorthand_euro)\\s*"
          + "(?<value>(?&number))\\s*"
          + "(?<units>(?&wt_units))?";

  private static final Pattern WT_SHORTHAND_EURO_PT = initPattern(WT_SHORTHAND_EURO);

  // A notation using 'fa'. It can be shorter than the other shorthand notations
  private static final String WT_FA = "fa\\d*-(?<value>(?&number))\\s*(?<units>(?&wt_units))?";

  private static final Pattern WT_FA_PT = initPattern(WT_FA);

  // Now we can look for the body mass, RANGE, optional units
  private static final String WT_KEY_AMBIGUOUS =
      "(?<key>(?&wt_key_word))(?&key_end)(?<value>(?&range))\\s*(?<units>(?&wt_units))?";

  private static final Pattern WT_KEY_AMBIGUOUS_PT = initPattern(WT_KEY_AMBIGUOUS);

  private static final Pattern UNITS_FROM_KEY = Pattern.compile("(grams)$");

  @VisibleForTesting protected static final String TOTAL_WEIGHT = "total weight";

  private static final List<Pattern> PATTERNS =
      Arrays.asList(
          EN_WT_PT,
          TOTAL_WT_KEY_PT,
          OTHER_WT_KEY_PT,
          KEY_UNITS_REQ_PT,
          WT_IN_PHRASE_PT,
          WT_KEY_WORD_PT,
          WT_KEY_WORD_REQ_PT,
          WT_SHORTHAND_PT,
          WT_SHORTHAND_REQ_PT,
          WT_SHORTHAND_EURO_PT,
          WT_FA_PT,
          WT_KEY_AMBIGUOUS_PT);

  private static void put(String key, String value) {
    String result = value;
    for (Map.Entry<String, String> entry : RegexDefault.get().entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    R_MAP.put(key, result);
  }

  private static Pattern initPattern(String value) {
    String result = value;
    for (Map.Entry<String, String> entry : R_MAP.entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : RegexDefault.get().entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    return Pattern.compile(result);
  }

  private static String getGroup(Matcher matcher, String value, String... group) {
    String result = null;
    for (String g : group) {
      if (matcher.pattern().pattern().contains("<" + g + ">")) {
        String s = matcher.group(g);
        if (result == null) {
          result = s;
        } else {
          result = String.join(", ", result, s);
        }
      }
    }
    return result == null ? value : result;
  }

  public static Optional<DynamicProperty> parse(String source) {
    if (source == null || source.isEmpty()) {
      return Optional.empty();
    }

    try {
      for (Pattern p : PATTERNS) {

        Matcher matcher = p.matcher(source.toLowerCase());

        if (matcher.find()) {
          String key = getGroup(matcher, TOTAL_WEIGHT, "key");
          String value = getGroup(matcher, null, "value", "value1", "value2");
          String units = getGroup(matcher, null, "units", "units1", "units2");
          if (key != null && units == null) {
            Matcher km = UNITS_FROM_KEY.matcher(key);
            units = km.find() ? "grams" : null;
          }

          if (key != null && value != null && TOTAL_WEIGHT_SET.contains(key)) {
            DynamicProperty dynamicProperty =
                DynamicProperty.newBuilder()
                    .setKey(TOTAL_WEIGHT)
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
