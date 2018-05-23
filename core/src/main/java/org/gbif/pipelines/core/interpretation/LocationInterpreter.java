package org.gbif.pipelines.core.interpretation;

import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpretation.Interpretation.Trace;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.LocationParser;
import org.gbif.pipelines.core.parsers.location.ParsedLocation;
import org.gbif.pipelines.core.utils.StringUtil;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueType;
import org.gbif.pipelines.io.avro.Location;

import java.util.Objects;
import java.util.function.Function;

import com.google.common.base.Strings;

import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_PRECISION_LOWER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_PRECISION_UPPER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND;

public interface LocationInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * Interprets the {@link DwcTerm#country}, {@link DwcTerm#countryCode}, {@link DwcTerm#decimalLatitude} and the
   * {@link DwcTerm#decimalLongitude} terms.
   */
  static LocationInterpreter interpretCountryAndCoordinates(Location locationRecord, String wsPropertiesPath) {
    return (ExtendedRecord extendedRecord) -> {

      // parse the terms
      ParsedField<ParsedLocation> parsedResult = LocationParser.parse(extendedRecord, wsPropertiesPath);

      // set values in the location record
      ParsedLocation parsedLocation = parsedResult.getResult();
      if (Objects.nonNull(parsedLocation.getCountry())) {
        locationRecord.setCountry(parsedLocation.getCountry().getTitle());
        locationRecord.setCountryCode(parsedLocation.getCountry().getIso2LetterCode());
      }

      if (Objects.nonNull(parsedLocation.getLatLng())) {
        locationRecord.setDecimalLatitude(parsedLocation.getLatLng().getLat());
        locationRecord.setDecimalLongitude(parsedLocation.getLatLng().getLng());
      }

      // create the interpretation
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      // set the issues to the interpretation
      parsedResult.getIssues().forEach(issue -> {
        Trace<IssueType> trace;
        if (Objects.nonNull(issue.getTerms()) && !issue.getTerms().isEmpty() && Objects.nonNull(issue.getTerms().get(0))) {
          // FIXME: now we take the first term. Should Trace accept a list of terms??
          trace = Trace.of(issue.getTerms().get(0).simpleName(), issue.getIssueType());
        } else {
          trace = Trace.of(issue.getIssueType());
        }

        interpretation.withValidation(trace);
      });

      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#continent} interpretation.
   */
  static LocationInterpreter interpretContinent(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.continentParser().map(extendedRecord, parseResult -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      if (parseResult.isSuccessful()) {
        locationRecord.setContinent(parseResult.getPayload().name());
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.continent.name(), IssueType.CONTINENT_INVALID));
      }
      return interpretation;
    }).orElse(Interpretation.of(extendedRecord));
  }

  /**
   * {@link DwcTerm#waterBody} interpretation.
   */
  static LocationInterpreter interpretWaterBody(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.waterBody.qualifiedName());
      if (!Strings.isNullOrEmpty(value)) {
        locationRecord.setWaterBody(StringUtil.cleanName(value));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#stateProvince} interpretation.
   */
  static LocationInterpreter interpretStateProvince(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.stateProvince.qualifiedName());
      if (!Strings.isNullOrEmpty(value)) {
        locationRecord.setStateProvince(StringUtil.cleanName(value));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#minimumElevationInMeters} interpretation.
   */
  static LocationInterpreter interpretMinimumElevationInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.minimumElevationInMeters.qualifiedName());
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        locationRecord.setMinimumElevationInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.minimumElevationInMeters.name(),
                                               IssueType.MIN_ELEVATION_INVALID));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#maximumElevationInMeters} interpretation.
   */
  static LocationInterpreter interpretMaximumElevationInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.maximumElevationInMeters.qualifiedName());
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        locationRecord.setMaximumElevationInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.maximumElevationInMeters.name(),
                                               IssueType.MAX_ELEVATION_INVALID));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#minimumDepthInMeters} interpretation.
   */
  static LocationInterpreter interpretMinimumDepthInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.minimumDepthInMeters.qualifiedName());
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        locationRecord.setMinimumDepthInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.minimumDepthInMeters.name(), IssueType.MIN_DEPTH_INVALID));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#maximumDepthInMeters} interpretation.
   */
  static LocationInterpreter interpretMaximumDepthInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.maximumDepthInMeters.qualifiedName());
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        locationRecord.setMaximumDepthInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.maximumDepthInMeters.name(), IssueType.MAX_DEPTH_INVALID));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#maximumDepthInMeters} interpretation.
   */
  static LocationInterpreter interpretMinimumDistanceAboveSurfaceInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName());
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        locationRecord.setMinimumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.minimumDistanceAboveSurfaceInMeters.name(),
                                               IssueType.MIN_DISTANCE_ABOVE_SURFACE_INVALID));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#maximumDepthInMeters} interpretation.
   */
  static LocationInterpreter interpretMaximumDistanceAboveSurfaceInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName());
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        locationRecord.setMaximumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.maximumDistanceAboveSurfaceInMeters.name(),
                                               IssueType.MAX_DISTANCE_ABOVE_SURFACE_INVALID));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#coordinateUncertaintyInMeters} interpretation.
   */
  static LocationInterpreter interpretCoordinateUncertaintyInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.coordinateUncertaintyInMeters.qualifiedName());
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      Double result = parseResult.isSuccessful() ? Math.abs(parseResult.getPayload()) : null;
      if (Objects.nonNull(result)
          && result > COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND
          && result < COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND) {
        locationRecord.setCoordinateUncertaintyInMeters(result);
      } else {
        interpretation.withValidation(Trace.of(DwcTerm.coordinateUncertaintyInMeters.name(),
                                               IssueType.COORDINATE_UNCERTAINTY_METERS_INVALID));
      }
      return interpretation;
    };
  }

  /**
   * {@link DwcTerm#coordinatePrecision} interpretation.
   */
  static LocationInterpreter interpretCoordinatePrecision(Location locationRecord) {
    return (ExtendedRecord extendedRecord) ->
      SimpleTypeParser.parseDouble(extendedRecord, DwcTerm.coordinatePrecision, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        Double result = parseResult.orElse(null);
        if (Objects.nonNull(result) && result >= COORDINATE_PRECISION_LOWER_BOUND && result <=
                                                                            COORDINATE_PRECISION_UPPER_BOUND) {
          locationRecord.setCoordinatePrecision(result);
        } else {
          interpretation.withValidation(Trace.of(DwcTerm.coordinatePrecision.name(), IssueType.COORDINATE_PRECISION_INVALID));
        }
        return interpretation;
      }).orElse(Interpretation.of(extendedRecord));
  }

}
