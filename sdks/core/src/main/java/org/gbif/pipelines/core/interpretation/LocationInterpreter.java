package org.gbif.pipelines.core.interpretation;

import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.Context;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.LocationParser;
import org.gbif.pipelines.core.parsers.location.ParsedLocation;
import org.gbif.pipelines.core.utils.StringUtil;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;

import java.util.Objects;
import java.util.function.BiConsumer;

import com.google.common.base.Strings;

import static org.gbif.api.vocabulary.OccurrenceIssue.CONTINENT_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_PRECISION_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

public class LocationInterpreter {

  // COORDINATE_UNCERTAINTY_METERS bounds are exclusive bounds
  private static final double COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND = 0d;
  // 5000 km seems safe
  private static final double COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND = 5_000_000d;

  private static final double COORDINATE_PRECISION_LOWER_BOUND = 0d;
  // 45 close to 5000 km
  private static final double COORDINATE_PRECISION_UPPER_BOUND = 45d;

  private LocationInterpreter() {}

  public static Context<ExtendedRecord, LocationRecord> createContext(ExtendedRecord er) {
    LocationRecord lr = LocationRecord.newBuilder().setId(er.getId()).build();
    return new Context<>(er, lr);
  }

  /**
   * Interprets the {@link DwcTerm#country}, {@link DwcTerm#countryCode}, {@link
   * DwcTerm#decimalLatitude} and the {@link DwcTerm#decimalLongitude} terms.
   */
  public static BiConsumer<ExtendedRecord, LocationRecord> interpretCountryAndCoordinates(
      Config wsConfig) {
    return (er, lr) -> {
      // parse the terms
      ParsedField<ParsedLocation> parsedResult = LocationParser.parse(er, wsConfig);

      // set values in the location record
      ParsedLocation parsedLocation = parsedResult.getResult();
      if (Objects.nonNull(parsedLocation.getCountry())) {
        lr.setCountry(parsedLocation.getCountry().getTitle());
        lr.setCountryCode(parsedLocation.getCountry().getIso2LetterCode());
      }

      if (Objects.nonNull(parsedLocation.getLatLng())) {
        lr.setDecimalLatitude(parsedLocation.getLatLng().getLat());
        lr.setDecimalLongitude(parsedLocation.getLatLng().getLng());
      }

      // set the issues to the interpretation
      addIssue(lr, parsedResult.getIssues());
    };
  }

  /** {@link DwcTerm#continent} interpretation. */
  public static void interpretContinent(ExtendedRecord er, LocationRecord lr) {
    VocabularyParsers.continentParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                lr.setContinent(parseResult.getPayload().name());
              } else {
                addIssue(lr, CONTINENT_INVALID);
              }
              return lr;
            });
  }

  /** {@link DwcTerm#waterBody} interpretation. */
  public static void interpretWaterBody(ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.waterBody);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setWaterBody(StringUtil.cleanName(value));
    }
  }

  /** {@link DwcTerm#stateProvince} interpretation. */
  public static void interpretStateProvince(ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.stateProvince);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setStateProvince(StringUtil.cleanName(value));
    }
  }

  /** {@link DwcTerm#minimumElevationInMeters} interpretation. */
  public static void interpretMinimumElevationInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.minimumElevationInMeters);
    ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
    if (parseResult.isSuccessful()) {
      lr.setMinimumElevationInMeters(parseResult.getPayload());
    } else {
      addIssue(lr, "MIN_ELEVATION_INVALID");
    }
  }

  /** {@link DwcTerm#maximumElevationInMeters} interpretation. */
  public static void interpretMaximumElevationInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.maximumElevationInMeters);
    ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
    if (parseResult.isSuccessful()) {
      lr.setMaximumElevationInMeters(parseResult.getPayload());
    } else {
      addIssue(lr, "MAX_ELEVATION_INVALID");
    }
  }

  /** {@link DwcTerm#minimumDepthInMeters} interpretation. */
  public static void interpretMinimumDepthInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.minimumDepthInMeters);
    ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
    if (parseResult.isSuccessful()) {
      lr.setMinimumDepthInMeters(parseResult.getPayload());
    } else {
      addIssue(lr, "MIN_DEPTH_INVALID");
    }
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  public static void interpretMaximumDepthInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.maximumDepthInMeters);
    ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
    if (parseResult.isSuccessful()) {
      lr.setMaximumDepthInMeters(parseResult.getPayload());
    } else {
      addIssue(lr, "MAX_DEPTH_INVALID");
    }
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  public static void interpretMinimumDistanceAboveSurfaceInMeters(
      ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.minimumDistanceAboveSurfaceInMeters);
    ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
    if (parseResult.isSuccessful()) {
      lr.setMinimumDistanceAboveSurfaceInMeters(parseResult.getPayload());
    } else {
      addIssue(lr, "MIN_DISTANCE_ABOVE_SURFACE_INVALID");
    }
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  public static void interpretMaximumDistanceAboveSurfaceInMeters(
      ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.maximumDistanceAboveSurfaceInMeters);
    ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
    if (parseResult.isSuccessful()) {
      lr.setMaximumDistanceAboveSurfaceInMeters(parseResult.getPayload());
    } else {
      addIssue(lr, "MAX_DISTANCE_ABOVE_SURFACE_INVALID");
    }
  }

  /** {@link DwcTerm#coordinateUncertaintyInMeters} interpretation. */
  public static void interpretCoordinateUncertaintyInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractValue(er, DwcTerm.coordinateUncertaintyInMeters);
    ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
    Double result = parseResult.isSuccessful() ? Math.abs(parseResult.getPayload()) : null;
    if (Objects.nonNull(result)
        && result > COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND
        && result < COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND) {
      lr.setCoordinateUncertaintyInMeters(result);
    } else {
      addIssue(lr, COORDINATE_UNCERTAINTY_METERS_INVALID);
    }
  }

  /** {@link DwcTerm#coordinatePrecision} interpretation. */
  public static void interpretCoordinatePrecision(ExtendedRecord er, LocationRecord lr) {
    SimpleTypeParser.parseDouble(
        er,
        DwcTerm.coordinatePrecision,
        parseResult -> {
          Double result = parseResult.orElse(null);
          if (Objects.nonNull(result)
              && result >= COORDINATE_PRECISION_LOWER_BOUND
              && result <= COORDINATE_PRECISION_UPPER_BOUND) {
            lr.setCoordinatePrecision(result);
          } else {
            addIssue(lr, COORDINATE_PRECISION_INVALID);
          }
        });
  }
}
