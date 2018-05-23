package org.gbif.pipelines.core.parsers.location;

import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.InterpretationIssue;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.core.parsers.legacy.CoordinateParseUtils;
import org.gbif.pipelines.core.parsers.memoize.ParserMemoizer;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueType;

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.Lists;

/**
 * Parser for the Dwc Terms related with the coordinates.
 */
public class CoordinatesParser {

  private static final ParserMemoizer<RawLatLng,ParsedField<LatLng>> LAT_LNG_PARSER =
    ParserMemoizer.memoize(((RawLatLng rawLatLng) -> CoordinateParseUtils.parseLatLng(rawLatLng.getLat(), rawLatLng.getLng())));

  private static final ParserMemoizer<String,ParsedField<LatLng>> VERTBATIM_COORD_PARSER =
    ParserMemoizer.memoize(CoordinateParseUtils::parseVerbatimCoordinates, ParsedField.fail(InterpretationIssue.of(IssueType.COORDINATE_INVALID, DwcTerm.verbatimCoordinates)));

  // parses decimal latitude and longitude fields
  private static final Function<ExtendedRecord, ParsedField<LatLng>> DECIMAL_LAT_LNG_FN =
    (extendedRecord -> LAT_LNG_PARSER.parse(RawLatLng.of(extendedRecord.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()),
                                                        extendedRecord.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()))));
  // parses verbatim latitude and longitude fields
  private static final Function<ExtendedRecord, ParsedField<LatLng>> VERBATIM_LAT_LNG_FN =
    (extendedRecord -> LAT_LNG_PARSER.parse(RawLatLng.of(extendedRecord.getCoreTerms().get(DwcTerm.verbatimLatitude.qualifiedName()),
                                                        extendedRecord.getCoreTerms().get(DwcTerm.verbatimLongitude.qualifiedName()))));
  // parses verbatim coordinates fields
  private static final Function<ExtendedRecord, ParsedField<LatLng>> VERBATIM_COORDS_FN =
    (extendedRecord -> VERTBATIM_COORD_PARSER.parse(extendedRecord.getCoreTerms().get(DwcTerm.verbatimCoordinates.qualifiedName())));

  // list with all the parsing functions
  private static final List<Function<ExtendedRecord, ParsedField<LatLng>>> PARSING_FUNCTIONS =
    Lists.newArrayList(DECIMAL_LAT_LNG_FN, VERBATIM_LAT_LNG_FN, VERBATIM_COORDS_FN);

  private CoordinatesParser() {}

  /**
   * Parses the coordinates fields of a {@link ExtendedRecord}.
   * <p>
   * It tries with these fields, in this order, and returns the first successful one:
   * <ol>
   * <li>{@link DwcTerm#decimalLatitude} and {@link DwcTerm#decimalLongitude}</li>
   * <li>{@link DwcTerm#verbatimLatitude} and {@link DwcTerm#verbatimLongitude}</li>
   * <li>{@link DwcTerm#verbatimCoordinates}</li>
   * </ol>
   *
   * @param extendedRecord {@link ExtendedRecord} with the fields to parse.
   *
   * @return {@link ParsedField<LatLng>} for the coordinates parsed.
   */
  public static ParsedField<LatLng> parseCoords(ExtendedRecord extendedRecord) {
    ParsedField<LatLng> result = null;
    for (Function<ExtendedRecord, ParsedField<LatLng>> parsingFunction : PARSING_FUNCTIONS) {
      result = parsingFunction.apply(extendedRecord);

      if (result.isSuccessful()) {
        // return the first successful result
        return result;
      }
    }

    return result;
  }

}
