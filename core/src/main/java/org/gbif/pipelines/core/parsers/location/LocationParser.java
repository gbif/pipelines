package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.InterpretationIssue;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.core.parsers.legacy.CoordinateParseUtils;
import org.gbif.pipelines.core.parsers.legacy.Wgs84Projection;
import org.gbif.pipelines.core.utils.AvroDataValidator;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LocationParser {

  // TODO: meter logs

  public static ParsedField<ParsedLocation> parseCountryAndCoordinates(ExtendedRecord extendedRecord) {
    AvroDataValidator.checkNullOrEmpty(extendedRecord);

    List<InterpretationIssue> issues = new ArrayList<>();

    // parse country
    ParsedField<Country> countryParsed = parseCountry(extendedRecord);

    if (!countryParsed.isSuccessful()) {
      issues.addAll(countryParsed.getIssues());
    }

    // parse country code
    ParsedField<Country> countryCodeParsed = parseCountryCode(extendedRecord);
    if (!countryCodeParsed.isSuccessful()) {
      issues.addAll(countryCodeParsed.getIssues());
    }

    // get results from previous parsers
    Optional<Country> countryName = Optional.ofNullable(countryParsed.getResult());
    Optional<Country> countryCode = Optional.ofNullable(countryCodeParsed.getResult());

    // check for a mismatch between the country and the country code
    if (countryName.isPresent() && countryCode.isPresent() && !countryName.get().equals(countryCode.get())) {
      issues.add(new InterpretationIssue(IssueType.COUNTRY_MISMATCH, DwcTerm.country, DwcTerm.countryCode));
    }

    // set the country. We take the country code parsing as default
    Country countryMatched =
      countryCode.isPresent() ? countryCode.get() : countryName.isPresent() ? countryName.get() : null;

    // parse coordinates
    ParsedField<LatLng> coordsParsed = parseLatLng(extendedRecord);

    // return if coordinates parsing failed
    if (!coordsParsed.isSuccessful()) {
      issues.addAll(coordsParsed.getIssues());
      ParsedLocation parsedLocation = ParsedLocation.newBuilder().country(countryMatched).build();
      return ParsedField.fail(parsedLocation, issues);
    }

    // get result and issues from coordinates parsing
    LatLng latLng = coordsParsed.getResult();
    issues.addAll(coordsParsed.getIssues());

    // set current parsed valuies
    ParsedLocation parsedLocation = ParsedLocation.newBuilder().country(countryMatched).latLng(latLng).build();

    // if the coords parsing was succesfull we try to do a country match with the coordinates
    ParsedField<ParsedLocation> match =
      LocationMatcher.newMatcher(parsedLocation.getLatLng(), parsedLocation.getCountry())
        .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LAT)
        .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_LNG)
        .addAdditionalTransform(CoordinatesTransformation.PRESUMED_NEGATED_COORDS)
        .addAdditionalTransform(CoordinatesTransformation.PRESUMED_SWAPPED_COORDS)
        .applyMatch();

    // if the match succeed we use it as a result
    if (match.isSuccessful()) {
      issues.addAll(match.getIssues());
      return ParsedField.<ParsedLocation>newBuilder().successful(match.isSuccessful() && countryMatched != null)
        .result(match.getResult())
        .issues(issues)
        .build();
    }

    // if match failed we return the previous values
    return ParsedField.<ParsedLocation>newBuilder().successful(match.isSuccessful() && countryMatched != null)
      .result(parsedLocation)
      .issues(issues)
      .build();
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
    final String decimalLatitudeVerbatim = extendedRecord.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName());
    final String decimalLongitudeVerbatim = extendedRecord.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName());

    ParsedField<LatLng> parsedLatLon =
      CoordinateParseUtils.parseLatLng(decimalLatitudeVerbatim, decimalLongitudeVerbatim);

    // collect issues form the coords parsing
    List<InterpretationIssue> issues = parsedLatLon.getIssues();

    ParsedField.Builder<LatLng> builder = ParsedField.newBuilder();
    if (!parsedLatLon.isSuccessful()) {
      // coords parsing failed
      return builder.issues(issues).build();
    }

    // interpret geodetic datum and reproject if needed
    // the reprojection will keep the original values even if it failed with issues
    final String datumVerbatim = extendedRecord.getCoreTerms().get(DwcTerm.geodeticDatum.qualifiedName());
    ParsedField<LatLng> projectedLatLon =
      Wgs84Projection.reproject(parsedLatLon.getResult().getLat(), parsedLatLon.getResult().getLng(), datumVerbatim);

    // collect issues from the projection parsing
    issues.addAll(projectedLatLon.getIssues());

    return builder.successful(true).result(projectedLatLon.getResult()).issues(issues).build();
  }

}
