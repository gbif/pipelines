package au.org.ala.pipelines.parser;

import com.google.common.base.Strings;
import java.util.UnknownFormatConversionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;

/**
 * This is a port of
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/main/scala/au/org/ala/biocache/parser/DistanceRangeParser.scala
 */
@Slf4j
public class DistanceParser extends MeterRangeParser {

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
    Matcher glMatcher = Pattern.compile(greaterOrLessThan).matcher(normalised);
    if (glMatcher.find()) {
      String numberStr = glMatcher.group(2);
      String uom = glMatcher.group(3);
      parseStr = numberStr + uom;
    }
    // range check
    Matcher rangeMatcher = Pattern.compile(range).matcher(normalised);
    if (rangeMatcher.find()) {
      String numberStr = rangeMatcher.group(3);
      String uom = rangeMatcher.group(4);
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

    if (!Strings.isNullOrEmpty(parseStr)) {
      ParseResult<Double> iMeter = MeterRangeParser.parseMeters(parseStr);
      if (iMeter.isSuccessful()) {
        return iMeter.getPayload();
      }
    }

    throw new UnknownFormatConversionException("Uncertainty: " + value + " cannot be parsed!");
  }
}
