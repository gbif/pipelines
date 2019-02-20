package org.gbif.pipelines.parsers.parsers.location;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.parsers.VocabularyParser;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.location.legacy.Wgs84Projection;
import org.gbif.pipelines.parsers.utils.ModelUtils;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_MISMATCH;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

/**
 * Parses the location fields.
 *
 * <p>Currently, it parses the country, countryCode and coordinates together.
 */
public class LocationParser {

  private LocationParser() {}

  public static ParsedField<ParsedLocation> parse(ExtendedRecord er, KeyValueStore<LatLng, String> kvStore) {
    ModelUtils.checkNullOrEmpty(er);
    Objects.requireNonNull(kvStore, "Geolookup kvStore is required");

    List<String> issues = new ArrayList<>();

    // Parse country
    ParsedField<Country> parsedCountry = parseCountry(er, VocabularyParser.countryParser(), COUNTRY_INVALID.name());
    Optional<Country> countryName = getResult(parsedCountry, issues);

    // Parse country code
    ParsedField<Country> parsedCountryCode = parseCountry(er, VocabularyParser.countryCodeParser(), COUNTRY_INVALID.name());
    Optional<Country> countryCode = getResult(parsedCountryCode, issues);

    // Check for a mismatch between the country and the country code
    if (!countryName.equals(countryCode)) {
      issues.add(COUNTRY_MISMATCH.name());
    }

    // Get the final country from the 2 previous parsings. We take the country code parsed as
    // default
    Country countryMatched = countryCode.orElseGet(() -> countryName.orElse(null));

    // Parse coordinates
    ParsedField<LatLng> coordsParsed = parseLatLng(er);

    // Add issues from coordinates parsing
    issues.addAll(coordsParsed.getIssues());

    // Return if coordinates parsing failed
    if (!coordsParsed.isSuccessful()) {
      ParsedLocation parsedLocation = new ParsedLocation(countryMatched, null);
      return ParsedField.fail(parsedLocation, issues);
    }

    // Set current parsed values
    ParsedLocation parsedLocation = new ParsedLocation(countryMatched, coordsParsed.getResult());

    // If the coords parsing was successful we try to do a country match with the coordinates
    ParsedField<ParsedLocation> match =
        LocationMatcher.create(parsedLocation.getLatLng(), parsedLocation.getCountry(), kvStore)
            .additionalTransform(CoordinatesFunction.NEGATED_LAT_FN)
            .additionalTransform(CoordinatesFunction.NEGATED_LNG_FN)
            .additionalTransform(CoordinatesFunction.NEGATED_COORDS_FN)
            .additionalTransform(CoordinatesFunction.SWAPPED_COORDS_FN)
            .apply();

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

  private static ParsedField<Country> parseCountry(ExtendedRecord er, VocabularyParser<Country> parser, String issue) {
    Optional<ParseResult<Country>> parseResultOpt = parser.map(er, parseRes -> parseRes);

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

  private static Optional<Country> getResult(ParsedField<Country> field, List<String> issues) {
    if (!field.isSuccessful()) {
      issues.addAll(field.getIssues());
    }

    return Optional.ofNullable(field.getResult());
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
    Double lat = parsedLatLon.getResult().getLatitude();
    Double lng = parsedLatLon.getResult().getLongitude();

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
