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
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.IssueType;
import org.gbif.pipelines.io.avro.location.LocationRecord;

import java.util.Objects;
import java.util.function.BiConsumer;

import com.google.common.base.Strings;

import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_PRECISION_LOWER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_PRECISION_UPPER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND;
import static org.gbif.pipelines.core.utils.InterpretationUtils.extract;

public interface LocationInterpreter
    extends BiConsumer<ExtendedRecord, Interpretation<LocationRecord>> {

  static LocationInterpreter interpretId() {
    return (extendedRecord, interpretation) ->
        interpretation.getValue().setId(extendedRecord.getId());
  }

  /**
   * Interprets the {@link DwcTerm#country}, {@link DwcTerm#countryCode}, {@link
   * DwcTerm#decimalLatitude} and the {@link DwcTerm#decimalLongitude} terms.
   */
  static LocationInterpreter interpretCountryAndCoordinates(Config wsConfig) {
    return (extendedRecord, interpretation) -> {
      LocationRecord locationRecord = interpretation.getValue();

      // parse the terms
      ParsedField<ParsedLocation> parsedResult = LocationParser.parse(extendedRecord, wsConfig);

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

      // set the issues to the interpretation
      parsedResult
          .getIssues()
          .forEach(
              issue -> {
                Trace<IssueType> trace;
                if (Objects.nonNull(issue.getTerms())
                    && !issue.getTerms().isEmpty()
                    && Objects.nonNull(issue.getTerms().get(0))) {
                  // FIXME: now we take the first term. Should Trace accept a list of terms??
                  trace = Trace.of(issue.getTerms().get(0).simpleName(), issue.getIssueType());
                } else {
                  trace = Trace.of(issue.getIssueType());
                }

                interpretation.withValidation(trace);
              });
    };
  }

  /** {@link DwcTerm#continent} interpretation. */
  static LocationInterpreter interpretContinent() {
    return (extendedRecord, interpretation) ->
        VocabularyParsers.continentParser()
            .map(
                extendedRecord,
                parseResult -> {
                  if (parseResult.isSuccessful()) {
                    interpretation.getValue().setContinent(parseResult.getPayload().name());
                  } else {
                    interpretation.withValidation(
                        Trace.of(DwcTerm.continent.name(), IssueType.CONTINENT_INVALID));
                  }
                  return interpretation;
                });
  }

  /** {@link DwcTerm#waterBody} interpretation. */
  static LocationInterpreter interpretWaterBody() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.waterBody);
      if (!Strings.isNullOrEmpty(value)) {
        interpretation.getValue().setWaterBody(StringUtil.cleanName(value));
      }
    };
  }

  /** {@link DwcTerm#stateProvince} interpretation. */
  static LocationInterpreter interpretStateProvince() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.stateProvince);
      if (!Strings.isNullOrEmpty(value)) {
        interpretation.getValue().setStateProvince(StringUtil.cleanName(value));
      }
    };
  }

  /** {@link DwcTerm#minimumElevationInMeters} interpretation. */
  static LocationInterpreter interpretMinimumElevationInMeters() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.minimumElevationInMeters);
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        interpretation.getValue().setMinimumElevationInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(
            Trace.of(DwcTerm.minimumElevationInMeters.name(), IssueType.MIN_ELEVATION_INVALID));
      }
    };
  }

  /** {@link DwcTerm#maximumElevationInMeters} interpretation. */
  static LocationInterpreter interpretMaximumElevationInMeters() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.maximumElevationInMeters);
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        interpretation.getValue().setMaximumElevationInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(
            Trace.of(DwcTerm.maximumElevationInMeters.name(), IssueType.MAX_ELEVATION_INVALID));
      }
    };
  }

  /** {@link DwcTerm#minimumDepthInMeters} interpretation. */
  static LocationInterpreter interpretMinimumDepthInMeters() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.minimumDepthInMeters);
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        interpretation.getValue().setMinimumDepthInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(
            Trace.of(DwcTerm.minimumDepthInMeters.name(), IssueType.MIN_DEPTH_INVALID));
      }
    };
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  static LocationInterpreter interpretMaximumDepthInMeters() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.maximumDepthInMeters);
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        interpretation.getValue().setMaximumDepthInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(
            Trace.of(DwcTerm.maximumDepthInMeters.name(), IssueType.MAX_DEPTH_INVALID));
      }
    };
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  static LocationInterpreter interpretMinimumDistanceAboveSurfaceInMeters() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.minimumDistanceAboveSurfaceInMeters);
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        interpretation.getValue().setMinimumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(
            Trace.of(
                DwcTerm.minimumDistanceAboveSurfaceInMeters.name(),
                IssueType.MIN_DISTANCE_ABOVE_SURFACE_INVALID));
      }
    };
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  static LocationInterpreter interpretMaximumDistanceAboveSurfaceInMeters() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.maximumDistanceAboveSurfaceInMeters);
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        interpretation.getValue().setMaximumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      } else {
        interpretation.withValidation(
            Trace.of(
                DwcTerm.maximumDistanceAboveSurfaceInMeters.name(),
                IssueType.MAX_DISTANCE_ABOVE_SURFACE_INVALID));
      }
    };
  }

  /** {@link DwcTerm#coordinateUncertaintyInMeters} interpretation. */
  static LocationInterpreter interpretCoordinateUncertaintyInMeters() {
    return (extendedRecord, interpretation) -> {
      String value = extract(extendedRecord, DwcTerm.coordinateUncertaintyInMeters);
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      Double result = parseResult.isSuccessful() ? Math.abs(parseResult.getPayload()) : null;
      if (Objects.nonNull(result)
          && result > COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND
          && result < COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND) {
        interpretation.getValue().setCoordinateUncertaintyInMeters(result);
      } else {
        interpretation.withValidation(
            Trace.of(
                DwcTerm.coordinateUncertaintyInMeters.name(),
                IssueType.COORDINATE_UNCERTAINTY_METERS_INVALID));
      }
    };
  }

  /** {@link DwcTerm#coordinatePrecision} interpretation. */
  static LocationInterpreter interpretCoordinatePrecision() {
    return (extendedRecord, interpretation) ->
        SimpleTypeParser.parseDouble(
            extendedRecord,
            DwcTerm.coordinatePrecision,
            parseResult -> {
              Double result = parseResult.orElse(null);
              if (Objects.nonNull(result)
                  && result >= COORDINATE_PRECISION_LOWER_BOUND
                  && result <= COORDINATE_PRECISION_UPPER_BOUND) {
                interpretation.getValue().setCoordinatePrecision(result);
              } else {
                interpretation.withValidation(
                    Trace.of(
                        DwcTerm.coordinatePrecision.name(),
                        IssueType.COORDINATE_PRECISION_INVALID));
              }
            });
  }
}
