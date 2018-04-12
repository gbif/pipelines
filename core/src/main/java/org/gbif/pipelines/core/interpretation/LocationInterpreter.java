package org.gbif.pipelines.core.interpretation;

import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.interpretation.Interpretation.Trace;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.core.parsers.location.LocationParser;
import org.gbif.pipelines.core.parsers.location.ParsedLocation;
import org.gbif.pipelines.core.utils.StringUtil;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueType;
import org.gbif.pipelines.io.avro.Location;

import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_PRECISION_LOWER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_PRECISION_UPPER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND;
import static org.gbif.pipelines.core.interpretation.Constant.Location.COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND;

public interface LocationInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * Interprets the {@link DwcTerm#country}, {@link DwcTerm#countryCode}, {@link DwcTerm#decimalLatitude} and the
   * {@link DwcTerm#decimalLongitude} terms.
   */
  static LocationInterpreter interpretCountryAndCoordinates(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {

      // parse the terms
      ParsedField<ParsedLocation> parsedResult = LocationParser.parseCountryAndCoordinates(extendedRecord);

      // set values in the location record
      ParsedLocation parsedLocation = parsedResult.getResult();
      if (parsedLocation.getCountry() != null) {
        locationRecord.setCountry(parsedLocation.getCountry().getTitle());
        locationRecord.setCountryCode(parsedLocation.getCountry().getIso2LetterCode());
      }

      if (parsedLocation.getLatLng() != null) {
        locationRecord.setDecimalLatitude(parsedLocation.getLatLng().getLat());
        locationRecord.setDecimalLongitude(parsedLocation.getLatLng().getLng());
      }

      // TODO: do we have to parse the datum here?? now it is just used in the coordinates interpretation

      // create the interpretation
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      // set the issues to the interpretation
      parsedResult.getIssues().forEach(issue -> {
        Term term = null;
        if (issue.getTerms() != null && !issue.getTerms().isEmpty()) {
          // FIXME: now we take the first term. Should Trace accept a list of terms??
          term = issue.getTerms().get(0);
        }

        interpretation.withValidation(Trace.of(term.simpleName(), issue.getIssueType()));
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
    }).get();
  }

  /**
   * {@link DwcTerm#waterBody} interpretation.
   */
  static LocationInterpreter interpretWaterBody(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DwcTerm.waterBody.qualifiedName());
      if (!StringUtils.isEmpty(value)) {
        locationRecord.setWaterBody(StringUtil.cleanName(value));
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
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value.trim());
      Double result = parseResult.isSuccessful() ? Math.abs(parseResult.getPayload()) : null;
      if (result != null
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
        if (result != null && result >= COORDINATE_PRECISION_LOWER_BOUND && result <= COORDINATE_PRECISION_UPPER_BOUND) {
          locationRecord.setCoordinatePrecision(result);
        } else {
          interpretation.withValidation(Trace.of(DwcTerm.coordinatePrecision.name(), IssueType.COORDINATE_PRECISION_INVALID));
        }
        return interpretation;
      });
  }

}
