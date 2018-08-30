package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.legacy.Wgs84Projection;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_MISMATCH;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

/**
 * Parses the location fields.
 *
 * <p>Currently, it parses the country, countryCode and coordinates together.
 */
public class LocationParser {

  private LocationParser() {}

  public static ParsedField<ParsedLocation> parse(ExtendedRecord extendedRecord, Config wsConfig) {
    ModelUtils.checkNullOrEmpty(extendedRecord);

    List<String> issues = new ArrayList<>();

    // Parse country
    ParsedField<Country> parsedCountry =
        parseCountry(extendedRecord, VocabularyParsers.countryParser(), COUNTRY_INVALID.name());
    Optional<Country> countryName = getResult(parsedCountry, issues);

    // Parse country code
    ParsedField<Country> parsedCountryCode =
        parseCountry(extendedRecord, VocabularyParsers.countryCodeParser(), COUNTRY_INVALID.name());
    Optional<Country> countryCode = getResult(parsedCountryCode, issues);

    // Check for a mismatch between the country and the country code
    if (!countryName.equals(countryCode)) {
      issues.add(COUNTRY_MISMATCH.name());
    }

    // Get the final country from the 2 previous parsings. We take the country code parsed as
    // default
    Country countryMatched = countryCode.orElseGet(() -> countryName.orElse(null));

    // Parse coordinates
    ParsedField<LatLng> coordsParsed = parseLatLng(extendedRecord);

    // Add issues from coordinates parsing
    issues.addAll(coordsParsed.getIssues());

    // Eeturn if coordinates parsing failed
    if (!coordsParsed.isSuccessful()) {
      ParsedLocation parsedLocation = ParsedLocation.newBuilder().country(countryMatched).build();
      return ParsedField.fail(parsedLocation, issues);
    }

    // Set current parsed values
    ParsedLocation parsedLocation =
        ParsedLocation.newBuilder()
            .country(countryMatched)
            .latLng(coordsParsed.getResult())
            .build();

    // If the coords parsing was succesful we try to do a country match with the coordinates
    ParsedField<ParsedLocation> match =
        LocationMatcher.newMatcher(
                parsedLocation.getLatLng(), parsedLocation.getCountry(), wsConfig)
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .addAdditionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .addAdditionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .addAdditionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .applyMatch();

    // Collect issues from the match
    issues.addAll(match.getIssues());

    if (match.isSuccessful()) {
      // If match succeed we take it as result
      parsedLocation = match.getResult();
    }

    // If the match succeed we use it as a result
    boolean isParsingSuccessful = match.isSuccessful() && countryMatched != null;

    return ParsedField.<ParsedLocation>newBuilder()
        .successful(isParsingSuccessful)
        .result(parsedLocation)
        .issues(issues)
        .build();
  }

  private static ParsedField<Country> parseCountry(
      ExtendedRecord extendedRecord, VocabularyParsers<Country> parser, String issue) {
    Optional<ParseResult<Country>> parseResultOpt =
        parser.map(extendedRecord, parseRes -> parseRes);

    if (!parseResultOpt.isPresent()) {
      // case when the country is null in the extended record. We return an issue not to break the
      // whole interpretation
      return ParsedField.<Country>newBuilder().withIssue(issue).build();
    }

    ParseResult<Country> parseResult = parseResultOpt.get();
    ParsedField.Builder<Country> builder = ParsedField.newBuilder();
    if (parseResult.isSuccessful()) {
      builder.successful(true);
      builder.result(parseResult.getPayload());
    } else {
      builder.withIssue(issue);
    }
    return builder.build();
  }

  private static Optional<Country> getResult(ParsedField<Country> parse, List<String> issues) {
    if (!parse.isSuccessful()) {
      issues.addAll(parse.getIssues());
    }

    return Optional.ofNullable(parse.getResult());
  }

  private static ParsedField<LatLng> parseLatLng(ExtendedRecord er) {
    ParsedField<LatLng> parsedLatLon = CoordinatesParser.parseCoords(er);

    // collect issues form the coords parsing
    List<String> issues = parsedLatLon.getIssues();

    if (!parsedLatLon.isSuccessful()) {
      // coords parsing failed
      return ParsedField.<LatLng>newBuilder().issues(issues).build();
    }

    // interpret geodetic datum and reproject if needed
    // the reprojection will keep the original values even if it failed with issues
    String datumVerbatim = extractValue(er, DwcTerm.geodeticDatum);
    Double lat = parsedLatLon.getResult().getLat();
    Double lng = parsedLatLon.getResult().getLng();

    ParsedField<LatLng> projectedLatLng = Wgs84Projection.reproject(lat, lng, datumVerbatim);

    // collect issues from the projection parsing
    issues.addAll(projectedLatLng.getIssues());

    return ParsedField.<LatLng>newBuilder()
        .successful(true)
        .result(projectedLatLng.getResult())
        .issues(issues)
        .build();
  }
}
