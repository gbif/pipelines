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
import org.gbif.pipelines.io.avro.IssueType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the location fields.
 * <p>
 * Currently, it parses the country, countryCode and coordinates together.
 */
public class LocationParser {

  private static final Logger LOG = LoggerFactory.getLogger(LocationParser.class);

  private LocationParser() {}

  public static ParsedField<ParsedLocation> parseCountryAndCoordinates(ExtendedRecord extendedRecord) {
    AvroDataValidator.checkNullOrEmpty(extendedRecord);

    List<InterpretationIssue> issues = new ArrayList<>();

    // parse country
    Optional<Country> countryName = getCountryParsedAndCollectIssues(parseCountry(extendedRecord), issues);
    // parse country code
    Optional<Country> countryCode = getCountryParsedAndCollectIssues(parseCountryCode(extendedRecord), issues);

    // check for a mismatch between the country and the country code
    checkCountryMismatch(issues, countryName, countryCode);

    // get the final country from the 2 previous parsings.
    Country countryMatched = getFinalCountryMatch(countryName, countryCode);

    // parse coordinates
    ParsedField<LatLng> coordsParsed = parseLatLng(extendedRecord);

    // add issues from coordinates parsing
    issues.addAll(coordsParsed.getIssues());

    // return if coordinates parsing failed
    if (!coordsParsed.isSuccessful()) {
      LOG.info("coordinates parsing failed");
      ParsedLocation parsedLocation = ParsedLocation.newBuilder().country(countryMatched).build();
      return ParsedField.fail(parsedLocation, issues);
    }

    // set current parsed values
    ParsedLocation parsedLocation =
      ParsedLocation.newBuilder().country(countryMatched).latLng(coordsParsed.getResult()).build();

    // if the coords parsing was succesful we try to do a country match with the coordinates
    ParsedField<ParsedLocation> match =
      LocationMatcher.newMatcher(parsedLocation.getLatLng(), parsedLocation.getCountry())
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_NEGATED_LAT)
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_NEGATED_LNG)
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_NEGATED_COORDS)
        .addAdditionalTransform(CoordinatesFunction.PRESUMED_SWAPPED_COORDS)
        .applyMatch();

    // collect issues from the match
    issues.addAll(match.getIssues());

    if (match.isSuccessful()) {
      // if match succeed we take it as result
      parsedLocation = match.getResult();
    }

    // if the match succeed we use it as a result
    return ParsedField.<ParsedLocation>newBuilder().successful(isParsingSuccessful(countryMatched, match))
      .result(parsedLocation)
      .issues(issues)
      .build();
  }

  private static Optional<Country> getCountryParsedAndCollectIssues(
    ParsedField<Country> countryParsed, List<InterpretationIssue> issues
  ) {
    if (!countryParsed.isSuccessful()) {
      LOG.info("country parsing failed");
      issues.addAll(countryParsed.getIssues());
    }

    return Optional.ofNullable(countryParsed.getResult());
  }

  private static Country getFinalCountryMatch(Optional<Country> countryName, Optional<Country> countryCode) {
    // We take the country code parsed as default
    return countryCode.orElseGet(() -> countryName.orElse(null));
  }

  private static boolean isParsingSuccessful(Country countryMatched, ParsedField<ParsedLocation> match) {
    return match.isSuccessful() && countryMatched != null;
  }

  private static void checkCountryMismatch(
    List<InterpretationIssue> issues, Optional<Country> countryName, Optional<Country> countryCode
  ) {
    if (!countryName.equals(countryCode)) {
      LOG.info("country mismatch found");
      issues.add(new InterpretationIssue(IssueType.COUNTRY_MISMATCH, DwcTerm.country, DwcTerm.countryCode));
    }
  }

  private static ParsedField<Country> parseCountry(ExtendedRecord extendedRecord) {
    Optional<ParseResult<Country>> parseResultOpt =
      VocabularyParsers.countryParser().map(extendedRecord, parseRes -> parseRes);

    if (!parseResultOpt.isPresent()) {
      // case when the country is null in the extended record. We return an issue not to break the whole interpretation
      return ParsedField.<Country>newBuilder().withIssue(new InterpretationIssue(IssueType.COUNTRY_INVALID,
                                                                                 DwcTerm.country)).build();
    }

    ParseResult<Country> parseResult = parseResultOpt.get();
    ParsedField.Builder<Country> builder = ParsedField.newBuilder();
    if (parseResult.isSuccessful()) {
      builder.successful(true);
      builder.result(parseResult.getPayload());
    } else {
      builder.withIssue(new InterpretationIssue(IssueType.COUNTRY_INVALID, DwcTerm.country));
    }
    return builder.build();
  }

  private static ParsedField<Country> parseCountryCode(ExtendedRecord extendedRecord) {
    Optional<ParseResult<Country>> parseResultOpt =
      VocabularyParsers.countryCodeParser().map(extendedRecord, parseRes -> parseRes);

    if (!parseResultOpt.isPresent()) {
      // case when the country is null in the extended record. We return an issue not to break the whole interpretation
      return ParsedField.<Country>newBuilder().withIssue(new InterpretationIssue(IssueType.COUNTRY_CODE_INVALID,
                                                                                 DwcTerm.countryCode)).build();
    }

    ParseResult<Country> parseResult = parseResultOpt.get();
    ParsedField.Builder<Country> builder = ParsedField.newBuilder();
    if (parseResult.isSuccessful()) {
      builder.successful(true);
      builder.result(parseResult.getPayload());
    } else {
      builder.withIssue(new InterpretationIssue(IssueType.COUNTRY_CODE_INVALID, DwcTerm.countryCode));
    }
    return builder.build();
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
    final String datumVerbatim = extendedRecord.getCoreTerms().get(DwcTerm.geodeticDatum.qualifiedName());
    ParsedField<LatLng> projectedLatLon =
      Wgs84Projection.reproject(parsedLatLon.getResult().getLat(), parsedLatLon.getResult().getLng(), datumVerbatim);

    // collect issues from the projection parsing
    issues.addAll(projectedLatLon.getIssues());

    return ParsedField.<LatLng>newBuilder().successful(true).result(projectedLatLon.getResult()).issues(issues).build();
  }

}