package au.org.ala.pipelines.parser;

import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.CoordinateParseUtils;
import org.gbif.pipelines.core.parsers.location.parser.Wgs84Projection;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Copy from org.gbif.pipelines.parsers.parsers.location.parser.CoordinateParser - - cannot be used
 * by outside package, since it is defined as private
 *
 * <p>New added: LOCATION_NOT_SUPPLIED assertion
 *
 * <p>
 *
 * <p>Parser for the Dwc Terms related to the coordinates.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CoordinatesParser {

  // parses decimal latitude and longitude fields
  private static final Function<ExtendedRecord, ParsedField<GeocodeRequest>> DECIMAL_LAT_LNG_FN =
      (er ->
          CoordinateParseUtils.parseLatLng(
              extractValue(er, DwcTerm.decimalLatitude),
              extractValue(er, DwcTerm.decimalLongitude)));

  // parses verbatim latitude and longitude fields
  private static final Function<ExtendedRecord, ParsedField<GeocodeRequest>> VERBATIM_LAT_LNG_FN =
      (er ->
          CoordinateParseUtils.parseLatLng(
              extractValue(er, DwcTerm.verbatimLatitude),
              extractValue(er, DwcTerm.verbatimLongitude)));

  // parses verbatim coordinates fields
  private static final Function<ExtendedRecord, ParsedField<GeocodeRequest>> VERBATIM_COORDS_FN =
      (er ->
          CoordinateParseUtils.parseVerbatimCoordinates(
              extractValue(er, DwcTerm.verbatimCoordinates)));

  // list with all the parsing functions
  private static final List<Function<ExtendedRecord, ParsedField<GeocodeRequest>>>
      PARSING_FUNCTIONS =
          Arrays.asList(DECIMAL_LAT_LNG_FN, VERBATIM_LAT_LNG_FN, VERBATIM_COORDS_FN);

  /**
   * Parses the coordinates fields of a {@link ExtendedRecord}.
   *
   * <p>
   *
   * <p>It tries with these fields, in this order, and returns the first successful one:
   *
   * <p>
   *
   * <ol>
   *   <li>{@link DwcTerm#decimalLatitude} and {@link DwcTerm#decimalLongitude}
   *   <li>{@link DwcTerm#verbatimLatitude} and {@link DwcTerm#verbatimLongitude}
   *   <li>{@link DwcTerm#verbatimCoordinates}
   * </ol>
   *
   * Coordinates will try to reproject WGS84. Always return coordinates with success or fail status
   *
   * @param extendedRecord {@link ExtendedRecord} with the fields to parse.
   * @return {@link ParsedField< GeocodeRequest >} for the coordinates parsed.
   */
  public static ParsedField<GeocodeRequest> parseCoords(ExtendedRecord extendedRecord) {
    Set<String> issues = new TreeSet<>();
    for (Function<ExtendedRecord, ParsedField<GeocodeRequest>> parsingFunction :
        PARSING_FUNCTIONS) {
      ParsedField<GeocodeRequest> result = parsingFunction.apply(extendedRecord);
      if (result.isSuccessful()) {
        // return the first successful result
        // Try to reproject, always returns lat/lng
        String geodeticDatum = extractValue(extendedRecord, DwcTerm.geodeticDatum);
        if (Strings.isNullOrEmpty(geodeticDatum)) {
          result.getIssues().add(ALAOccurrenceIssue.MISSING_GEODETICDATUM.name());
        }

        ParsedField<GeocodeRequest> projectedLatLng =
            Wgs84Projection.reproject(
                result.getResult().getLat(), result.getResult().getLng(), geodeticDatum);

        // Convert failure to success status with valid lat/lng
        // Add existing issues.
        projectedLatLng.getIssues().addAll(result.getIssues());
        return ParsedField.success(projectedLatLng.getResult(), projectedLatLng.getIssues());
      }
      issues.addAll(result.getIssues());
    }
    issues.add(ALAOccurrenceIssue.LOCATION_NOT_SUPPLIED.name());
    return ParsedField.fail(issues);
  }
}
