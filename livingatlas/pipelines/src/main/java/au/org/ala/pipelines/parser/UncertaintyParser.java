package au.org.ala.pipelines.parser;

import java.util.UnknownFormatConversionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import com.google.common.base.Strings;

/**
 * This is a port of
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/main/scala/au/org/ala/biocache/parser/DistanceRangeParser.scala
 */
@Slf4j
public class UncertaintyParser extends MeterRangeParser {

  static String singleNumber = "(-?[0-9]{1,})";
  static String decimalNumber = "(-?[0-9]{1,}[.]{1}[0-9]{1,})";
  static String range =
      "(-?[0-9.]{1,})([km|kilometres|kilometers|m|metres|meters|ft|feet|f]{0,})-([0-9.]{1,})([km|kilometres|kilometers|m|metres|meters|ft|feet|f]{0,})";
  static String greaterOrLessThan =
      "(\\>|\\<)(-?[0-9.]{1,})([km|kilometres|kilometers|m|metres|meters|ft|feet|f]{0,})";
  static String singleNumberMetres = "(-?[0-9]{1,})(m|metres|meters)";
  static String singleNumberKilometres = "(-?[0-9]{1,})(km|kilometres|kilometers)";
  static String singleNumberFeet = "(-?[0-9]{1,})(ft|feet|f)";
  static String singleNumberInch = "(-?[0-9]{1,})(in|inch|inches)";

  /**
   * Handle these formats: 2000 1km-10km 100m-1000m >10km >100m 100-1000 m Support inch/feet/km
   *
   * @return the value in metres and the original units
   */
  public static double parse(String value) {
    String normalised =
        value.replaceAll("\\[", "").replaceAll(",", "").replaceAll("]", "").toLowerCase().trim();

    String parseStr = "";

    if (normalised.matches(singleNumber + "|" + decimalNumber)) {
      parseStr = normalised + "m";
    }
    // less or great
    Matcher gl_matcher = Pattern.compile(greaterOrLessThan).matcher(normalised);
    if (gl_matcher.find()) {
      String numberStr = gl_matcher.group(2);
      String uom = gl_matcher.group(3);
      parseStr = numberStr + uom;
    }
    // range check
    Matcher range_matcher = Pattern.compile(range).matcher(normalised);
    if (range_matcher.find()) {
      String numberStr = range_matcher.group(3);
      String uom = range_matcher.group(4);
      parseStr = numberStr + uom;
    }

    if (normalised.matches(
        singleNumberMetres
            + "|"
            + singleNumberFeet
            + "|"
            + singleNumberKilometres
            + "|"
            + singleNumberInch)) {
      parseStr = normalised;
    }

    if ( !Strings.isNullOrEmpty(parseStr)) {
      ParseResult<Double> iMeter = MeterRangeParser.parseMeters(parseStr);
      if (iMeter.isSuccessful()) {
        return iMeter.getPayload();
      }
    }

    throw new UnknownFormatConversionException("Uncertainty: " + value + " cannot be parsed!");
  }
}
