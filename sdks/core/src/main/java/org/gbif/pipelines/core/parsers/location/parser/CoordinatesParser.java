package org.gbif.pipelines.core.parsers.location.parser;

import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.parsers.common.ParsedField;

/** Parser for the Dwc Terms related to the coordinates. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class CoordinatesParser {

  // parses decimal latitude and longitude fields
  private static final Function<ExtendedRecord, ParsedField<GeocodeRequest>> DECIMAL_LAT_LNG_FN =
      (er ->
          CoordinateParseUtils.parseLatLng(
                  er.extractValue(DwcTerm.decimalLatitude),
                  er.extractValue(DwcTerm.decimalLongitude)));

  // parses footprint WKT field (if it contains only a single point)
  private static final Function<ExtendedRecord, ParsedField<GeocodeRequest>> FOOTPRINT_WKT_FN =
      (er -> CoordinateParseUtils.parsePointFootprintWKT(er.extractValue(DwcTerm.footprintWKT)));

  // parses verbatim latitude and longitude fields
  private static final Function<ExtendedRecord, ParsedField<GeocodeRequest>> VERBATIM_LAT_LNG_FN =
      (er ->
          CoordinateParseUtils.parseLatLng(
              er.extractValue(DwcTerm.verbatimLatitude),
                  er.extractValue(DwcTerm.verbatimLongitude)));

  // parses verbatim coordinates fields
  private static final Function<ExtendedRecord, ParsedField<GeocodeRequest>> VERBATIM_COORDS_FN =
      (er ->
          CoordinateParseUtils.parseVerbatimCoordinates(
                  er.extractValue(DwcTerm.verbatimCoordinates)));

  // list with all the parsing functions
  private static final List<Function<ExtendedRecord, ParsedField<GeocodeRequest>>>
      PARSING_FUNCTIONS =
          Arrays.asList(DECIMAL_LAT_LNG_FN, VERBATIM_LAT_LNG_FN, VERBATIM_COORDS_FN);

  /**
   * Parses the coordinates fields of a {@link ExtendedRecord}.
   *
   * <p>It tries with these fields, in this order, and returns the first successful one:
   *
   * <ol>
   *   <li>{@link DwcTerm#decimalLatitude} and {@link DwcTerm#decimalLongitude}
   *   <li>{@link DwcTerm#verbatimLatitude} and {@link DwcTerm#verbatimLongitude}
   *   <li>{@link DwcTerm#verbatimCoordinates}
   * </ol>
   *
   * @param extendedRecord {@link ExtendedRecord} with the fields to parse.
   * @return {@link ParsedField< GeocodeRequest >} for the coordinates parsed.
   */
  static ParsedField<GeocodeRequest> parseCoords(ExtendedRecord extendedRecord) {
    Set<String> issues = new TreeSet<>();
    for (Function<ExtendedRecord, ParsedField<GeocodeRequest>> parsingFunction :
        PARSING_FUNCTIONS) {
      ParsedField<GeocodeRequest> result = parsingFunction.apply(extendedRecord);

      if (result.isSuccessful()) {
        // return the first successful result
        return result;
      }

      issues.addAll(result.getIssues());
    }

    return ParsedField.fail(issues);
  }

  /**
   * Parses coordinates in the footprintWKT field of a {@link ExtendedRecord}.
   *
   * <p>Non-POINT values in the footprint field are ignored. This method is separate from
   * parseCoords since footprintWKT has its own CRS in footprintSRS.
   *
   * @param extendedRecord {@link ExtendedRecord} with the fields to parse.
   * @return {@link ParsedField< GeocodeRequest >} for the coordinates parsed.
   */
  static ParsedField<GeocodeRequest> parseFootprint(ExtendedRecord extendedRecord) {
    Set<String> issues = new TreeSet<>();
    ParsedField<GeocodeRequest> result = FOOTPRINT_WKT_FN.apply(extendedRecord);

    if (result.isSuccessful()) {
      // return the first successful result
      return result;
    } else {
      issues.addAll(result.getIssues());
      return ParsedField.fail(issues);
    }
  }
}
