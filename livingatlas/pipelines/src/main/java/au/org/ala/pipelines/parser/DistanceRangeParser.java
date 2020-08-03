package au.org.ala.pipelines.parser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import java.util.UnknownFormatConversionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a port of
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/main/scala/au/org/ala/biocache/parser/DistanceRangeParser.scala
 */
@Slf4j
public class DistanceRangeParser extends MeterRangeParser{

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
   * Handle these formats: 2000 1km-10km 100m-1000m >10km >100m 100-1000 m
   *
   * @return the value in metres and the original units
   */
  public static double parse(String value) {
    String normalised =
        value.replaceAll("\\[", "").replaceAll(",", "").replaceAll("]", "").toLowerCase().trim();

    if (normalised.matches(singleNumber + "|" + decimalNumber)) {
      return Double.valueOf(normalised);
    }

    // Sequence of pattern match does matter
    Matcher gl_matcher = Pattern.compile(greaterOrLessThan).matcher(normalised);
    if (gl_matcher.find()) {
      String numberStr = gl_matcher.group(2);
      String uom = gl_matcher.group(3);
      return convertUOM(Double.valueOf(numberStr), uom);
    }

    // range check
    Matcher range_matcher = Pattern.compile(range).matcher(normalised);
    if (range_matcher.find()) {
      String numberStr = range_matcher.group(3);
      String uom = range_matcher.group(4);
      return convertUOM(Double.valueOf(numberStr), uom);
    }

    if(normalised.matches(singleNumberMetres + "|" + singleNumberFeet +"|" + singleNumberKilometres + "|" + singleNumberInch)){
      ParseResult<Double> iMeter = MeterRangeParser.parseMeters(normalised);
      if(iMeter.isSuccessful()){
        return iMeter.getPayload();
      }
    }

    throw new UnknownFormatConversionException("Uncertainty: " + value + " cannot be parsed!");
  }

  private static double convertUOM(double value, String uom) {
    switch (uom.toLowerCase()) {
      case "m":
      case "meters":
      case "metres":
      case "":
        return value;
      case "ft":
      case "feet":
      case "f":
        return value * 0.3048d;
      case "km":
      case "kilometers":
      case "kilometres":
        return value * 1000d;
      default:
        log.error("{} is not recognised UOM", uom);
        throw new UnknownFormatConversionException(uom + " is not recognised UOM");
    }
  }
}
