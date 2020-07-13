package au.org.ala.pipelines.parser;
import java.util.UnknownFormatConversionException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a port of
 * https://github.com/AtlasOfLivingAustralia/biocache-store/blob/master/src/main/scala/au/org/ala/biocache/parser/DistanceRangeParser.scala
 */
@Slf4j
public class DistanceRangeParser {

  static String singleNumber = "(-?[0-9]{1,})";
  static String decimalNumber = "(-?[0-9]{1,}[.]{1}[0-9]{1,})";
  static String range = "(-?[0-9.]{1,})([km|m|metres|meters|ft|feet]{0,})-([0-9.]{1,})([km|m|metres|meters|ft|feet]{0,})";
  static String greaterOrLessThan = "(\\>|\\<)(-?[0-9.]{1,})([km|m|metres|meters|ft|feet]{0,})";
  static String singleNumberMetres = "(-?[0-9]{1,})(m|metres|meters)";
  static String singleNumberKilometres = "(-?[0-9]{1,})(km|kilometres|kilometers)";
  static String singleNumberFeet = "(-?[0-9]{1,})(ft|feet|f)";

  /**
   * Handle these formats:
   * 2000
   * 1km-10km
   * 100m-1000m
   * >10km
   * >100m
   * 100-1000 m
   * @return the value in metres and the original units
   */
  public static double parse(String value){
    String normalised =  value.replaceAll("\\[", "").replaceAll(",","").replaceAll("]","").toLowerCase().trim();

    if (normalised.matches(singleNumber+"|"+decimalNumber)){
      return Double.valueOf(normalised);
    }

    //Sequence of pattern match does matter
    Matcher gl_matcher = Pattern.compile(greaterOrLessThan).matcher(normalised);
    if (gl_matcher.find()) {
      String numberStr = gl_matcher.group(2);
      String uom = gl_matcher.group(3);
      return convertUOM(Double.valueOf(numberStr), uom);
    }

    //range check
    Matcher range_matcher = Pattern.compile(range).matcher(normalised);
    if (range_matcher.find()) {
      String numberStr = range_matcher.group(3);
      String uom = range_matcher.group(4);
      return convertUOM(Double.valueOf(numberStr), uom);
    }

    //single number metres
    Matcher sm_matcher = Pattern.compile(singleNumberMetres).matcher(normalised);
    if (sm_matcher.find()) {
      String numberStr = sm_matcher.group(1);
      return  Double.valueOf(numberStr);
    }
    //single number feet
    Matcher sf_matcher = Pattern.compile(singleNumberFeet).matcher(normalised);
    if (sf_matcher.find()) {
      String numberStr = sf_matcher.group(1);
      return  convertUOM(Double.valueOf(numberStr), "ft");
    }

    //single number km
    Matcher skm_matcher = Pattern.compile(singleNumberKilometres).matcher(normalised);
    if (skm_matcher.find()) {
      String numberStr = skm_matcher.group(1);
      return convertUOM(Double.valueOf(numberStr), "km");
    }

    throw new UnknownFormatConversionException("Uncertainty: " + value + " cannot be parsed!");
  }

  private static double convertUOM(double value, String uom){
    switch (uom.toLowerCase()){
      case "m":
      case "":
        return value;
      case "ft":
        return value * 0.3048d;
      case "km":
        return value * 1000d;
      default:
        log.error("{} is not recognised UOM", uom);
        throw new UnknownFormatConversionException(uom + " is not recognised UOM");
    }
  }
}