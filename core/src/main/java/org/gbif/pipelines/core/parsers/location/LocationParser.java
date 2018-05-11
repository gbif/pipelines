package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.InterpretationIssue;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.core.parsers.legacy.Wgs84Projection;
import org.gbif.pipelines.core.utils.AvroDataValidator;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.dwc.terms.DwcTerm.country;
import static org.gbif.dwc.terms.DwcTerm.countryCode;
import static org.gbif.pipelines.io.avro.IssueType.COUNTRY_CODE_INVALID;
import static org.gbif.pipelines.io.avro.IssueType.COUNTRY_INVALID;
import static org.gbif.pipelines.io.avro.IssueType.COUNTRY_MISMATCH;

/**
 * Parses the location fields.
 * <p>
 * Currently, it parses the country, countryCode and coordinates together.
 */
public class LocationParser {

  private static final Logger LOG = LoggerFactory.getLogger(LocationParser.class);

  // Issues
  private static final InterpretationIssue COUNTRY_ISSUE = new InterpretationIssue(COUNTRY_INVALID, country);
  private static final InterpretationIssue COUNTRY_CODE_ISSUE = new InterpretationIssue(COUNTRY_CODE_INVALID, countryCode);
  private static final InterpretationIssue MISMATCH_ISSUE = new InterpretationIssue(COUNTRY_MISMATCH, country, countryCode);

  private LocationParser() {}

  public static ParsedField<ParsedLocation> parseCountryAndCoordinates(ExtendedRecord extendedRecord, String wsPropertiesPath) {
    AvroDataValidator.checkNullOrEmpty(extendedRecord);

    List<InterpretationIssue> issues = new ArrayList<>();

    // Parse country
    ParsedField<Country> parsedCountry = parse(extendedRecord, VocabularyParsers.countryParser(), COUNTRY_ISSUE);
    Optional<Country> countryName = getResult(parsedCountry, issues);

    // Parse country code
    ParsedField<Country> parsedCountryCode = parse(extendedRecord, VocabularyParsers.countryCodeParser(), COUNTRY_CODE_ISSUE);
    Optional<Country> countryCode = getResult(parsedCountryCode, issues);

    // Check for a mismatch between the country and the country code
    if (!countryName.equals(countryCode)) {
      LOG.debug("country mismatch found for country name {} and country code {}", countryName, countryCode);
      issues.add(MISMATCH_ISSUE);
    }

    // Get the final country from the 2 previous parsings. We take the country code parsed as default
    Country countryMatched = countryCode.orElseGet(() -> countryName.orElse(null));

    // Parse coordinates
    ParsedField<LatLng> coordsParsed = parseLatLng(extendedRecord);

    // Add issues from coordinates parsing
    issues.addAll(coordsParsed.getIssues());

    // Eeturn if coordinates parsing failed
    if (!coordsParsed.isSuccessful()) {
      LOG.debug("Parsing failed for coordinates {}", coordsParsed.getResult());
      ParsedLocation parsedLocation = ParsedLocation.newBuilder().country(countryMatched).build();
      return ParsedField.fail(parsedLocation, issues);
    }

    // Set current parsed values
    ParsedLocation parsedLocation =
      ParsedLocation.newBuilder().country(countryMatched).latLng(coordsParsed.getResult()).build();

    // If the coords parsing was succesful we try to do a country match with the coordinates
    ParsedField<ParsedLocation> match =
      LocationMatcher.newMatcher(parsedLocation.getLatLng(), parsedLocation.getCountry(), wsPropertiesPath)
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_NEGATED_LAT)
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_NEGATED_LNG)
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_NEGATED_COORDS)
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_SWAPPED_COORDS)
        .applyMatch();

    // Collect issues from the match
    issues.addAll(match.getIssues());

    if (match.isSuccessful()) {
      // If match succeed we take it as result
      parsedLocation = match.getResult();
    }

    // If the match succeed we use it as a result
    boolean isParsingSuccessful = match.isSuccessful() && Objects.nonNull(countryMatched);

    return ParsedField.<ParsedLocation>newBuilder().successful(isParsingSuccessful)
      .result(parsedLocation)
      .issues(issues)
      .build();
  }

  private static ParsedField<Country> parse(ExtendedRecord extendedRecord, VocabularyParsers<Country> parser, InterpretationIssue issue) {
    Optional<ParseResult<Country>> parseResultOpt = parser.map(extendedRecord, parseRes -> parseRes);

    if (!parseResultOpt.isPresent()) {
      // case when the country is null in the extended record. We return an issue not to break the whole interpretation
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

  private static Optional<Country> getResult(ParsedField<Country> parse, List<InterpretationIssue> issues) {
    if (!parse.isSuccessful()) {
      LOG.debug("Parsing failed for country {}", parse.getResult());
      issues.addAll(parse.getIssues());
    }

    return Optional.ofNullable(parse.getResult());
  }

  private static ParsedField<LatLng> parseLatLng(ExtendedRecord extendedRecord) {
    ParsedField<LatLng> parsedLatLon = CoordinatesParser.parseCoords(extendedRecord);

    // collect issues form the coords parsing
    List<InterpretationIssue> issues = parsedLatLon.getIssues();

    if (!parsedLatLon.isSuccessful()) {
      // coords parsing failed
      return ParsedField.<LatLng>newBuilder().issues(issues).build();
    }

    // interpret geodetic datum and reproject if needed
    // the reprojection will keep the original values even if it failed with issues
    String datumVerbatim = extendedRecord.getCoreTerms().get(DwcTerm.geodeticDatum.qualifiedName());
    Double lat = parsedLatLon.getResult().getLat();
    Double lng = parsedLatLon.getResult().getLng();

    ParsedField<LatLng> projectedLatLng = Wgs84Projection.reproject(lat, lng, datumVerbatim);

    // collect issues from the projection parsing
    issues.addAll(projectedLatLng.getIssues());

    return ParsedField.<LatLng>newBuilder().successful(true).result(projectedLatLng.getResult()).issues(issues).build();
  }

}