package org.gbif.pipelines.core.interpreters.core;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DoubleAccuracy;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.parsers.SimpleTypeParser;
import org.gbif.pipelines.parsers.parsers.VocabularyParser;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.location.parser.GadmParser;
import org.gbif.pipelines.parsers.parsers.location.parser.LocationParser;
import org.gbif.pipelines.parsers.parsers.location.parser.ParsedLocation;
import org.gbif.rest.client.geocode.GeocodeResponse;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.gbif.api.model.Constants.EBIRD_DATASET_KEY;
import static org.gbif.api.vocabulary.OccurrenceIssue.*;
import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractNullAwareValue;

/** Interprets the location terms of a {@link ExtendedRecord}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocationInterpreter {

  // COORDINATE_UNCERTAINTY_METERS bounds are exclusive bounds
  private static final double COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND = 0d;
  // 5000 km seems safe
  private static final double COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND = 5_000_000d;

  private static final double COORDINATE_PRECISION_LOWER_BOUND = 0d;
  // 45 close to 5000 km
  private static final double COORDINATE_PRECISION_UPPER_BOUND = 1d;

  //List of Geospatial Issues
  private static final Set<String> SPATIAL_ISSUES =
      new HashSet<>(Arrays.asList(
          ZERO_COORDINATE.name(),
          COORDINATE_INVALID.name(),
          COORDINATE_OUT_OF_RANGE.name(),
          COUNTRY_COORDINATE_MISMATCH.name()
      ));

  private static final CountryParser COUNTRY_PARSER = CountryParser.getInstance();

  /**
   * Determines if the record contains geo-spatial issues.
   */
  static boolean hasGeospatialIssues(LocationRecord lr) {
    return Optional.ofNullable(lr.getIssues())
        .map(il -> il.getIssueList().stream().anyMatch(SPATIAL_ISSUES::contains))
        .orElse(false);
  }

  /**
   * Interprets the {@link DwcTerm#country}, {@link DwcTerm#countryCode}, {@link
   * DwcTerm#decimalLatitude} and the {@link DwcTerm#decimalLongitude} terms.
   */
  public static BiConsumer<ExtendedRecord, LocationRecord> interpretCountryAndCoordinates(
      KeyValueStore<LatLng, GeocodeResponse> geocodeKvStore, MetadataRecord mdr) {
    return (er, lr) -> {
      if (geocodeKvStore != null) {
        // parse the terms
        ParsedField<ParsedLocation> parsedResult = LocationParser.parse(er, geocodeKvStore);

        // set values in the location record
        ParsedLocation parsedLocation = parsedResult.getResult();

        Optional.ofNullable(parsedLocation.getCountry())
            .ifPresent(
                country -> {
                  lr.setCountry(country.getTitle());
                  lr.setCountryCode(country.getIso2LetterCode());
                });

        LatLng latLng = parsedLocation.getLatLng();
        if (Objects.nonNull(latLng)) {
          lr.setDecimalLatitude(latLng.getLatitude());
          lr.setDecimalLongitude(latLng.getLongitude());
          lr.setHasCoordinate(Boolean.TRUE);
        } else {
          lr.setHasCoordinate(Boolean.FALSE);
        }

        // set the issues to the interpretation
        addIssue(lr, parsedResult.getIssues());

        // Has geo-spatial issues
        lr.setHasGeospatialIssue(hasGeospatialIssues(lr));

        if (mdr != null) {
          interpretPublishingCountry(er, mdr).ifPresent(lr::setPublishingCountry);
        }

        // Interpretation that required multiple sources
        // Determines if the record has been repatriated, i.e.: country != publishing Organization
        // Country.
        if (Objects.nonNull(lr.getCountryCode()) && Objects.nonNull(lr.getPublishingCountry())) {
          lr.setRepatriated(!lr.getCountryCode().equals(lr.getPublishingCountry()));
        }
      }
    };
  }

  /**
   * Uses the interpreted DwcTerm#decimalLatitude} and {@link DwcTerm#decimalLongitude} terms to
   * populate GADM administrative area GIDs.
   */
  public static BiConsumer<ExtendedRecord, LocationRecord> interpretGadm(
      KeyValueStore<LatLng, GeocodeResponse> geocodeKvStore) {
    return (er, lr) -> {
      if (geocodeKvStore != null && lr.getHasCoordinate()) {
        GadmParser.parseGadm(lr, geocodeKvStore).ifPresent(lr::setGadm);
      }
    };
  }

  /**
   * Interprets the publishing country for eBird dataset.
   */
  private static Optional<String> interpretPublishingCountry(ExtendedRecord er, MetadataRecord mr) {
    // Special case for eBird, use the supplied publishing country.
    if (EBIRD_DATASET_KEY.toString().equals(mr.getDatasetKey())) {

      String verbatimPublishingCountryCode = extractNullAwareValue(er, GbifTerm.publishingCountry);
      OccurrenceParseResult<Country> result =
          new OccurrenceParseResult<>(COUNTRY_PARSER.parse(verbatimPublishingCountryCode));

      if (result.isSuccessful()) {
        return Optional.of(result.getPayload().getIso2LetterCode());
      }
    }

    return Optional.ofNullable(mr.getDatasetPublishingCountry());
  }

  /** {@link DwcTerm#continent} interpretation. */
  public static void interpretContinent(ExtendedRecord er, LocationRecord lr) {
    Function<ParseResult<Continent>, LocationRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            lr.setContinent(parseResult.getPayload().name());
          } else {
            addIssue(lr, CONTINENT_INVALID);
          }
          return lr;
        };
    VocabularyParser.continentParser().map(er, fn);
  }

  /** {@link DwcTerm#waterBody} interpretation. */
  public static void interpretWaterBody(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.waterBody);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setWaterBody(cleanName(value));
    }
  }

  /** {@link DwcTerm#stateProvince} interpretation. */
  public static void interpretStateProvince(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.stateProvince);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setStateProvince(cleanName(value));
    }
  }

  /** {@link DwcTerm#minimumElevationInMeters} interpretation. */
  public static void interpretMinimumElevationInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.minimumElevationInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMinimumElevationInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#maximumElevationInMeters} interpretation. */
  public static void interpretMaximumElevationInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.maximumElevationInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMaximumElevationInMeters(parseResult.getPayload());
      }
    }
  }

  /**
   * {@link GbifTerm#elevation} and {@link GbifTerm#elevationAccuracy} interpretation.
   */
  public static void interpretElevation(ExtendedRecord er, LocationRecord lr) {
    String minElevation = extractNullAwareValue(er, DwcTerm.minimumElevationInMeters);
    String maxElevation = extractNullAwareValue(er, DwcTerm.maximumElevationInMeters);
    OccurrenceParseResult<DoubleAccuracy> occurrenceParseResult =
        MeterRangeParser.parseElevation(minElevation, maxElevation, null);
    if (occurrenceParseResult.isSuccessful()) {
      lr.setElevation(occurrenceParseResult.getPayload().getValue());
      lr.setElevationAccuracy(occurrenceParseResult.getPayload().getAccuracy());
      occurrenceParseResult.getIssues().forEach(i -> addIssue(lr, i));
    }
  }

  /** {@link DwcTerm#minimumDepthInMeters} interpretation. */
  public static void interpretMinimumDepthInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.minimumDepthInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMinimumDepthInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  public static void interpretMaximumDepthInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.maximumDepthInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMaximumDepthInMeters(parseResult.getPayload());
      }
    }
  }

  /**
   * {@link org.gbif.dwc.terms.GbifTerm#depth} and {@link org.gbif.dwc.terms.GbifTerm#depthAccuracy}
   * interpretation.
   */
  public static void interpretDepth(ExtendedRecord er, LocationRecord lr) {
    String minDepth = extractNullAwareValue(er, DwcTerm.minimumDepthInMeters);
    String maxDepth = extractNullAwareValue(er, DwcTerm.maximumDepthInMeters);
    OccurrenceParseResult<DoubleAccuracy> occurrenceParseResult = MeterRangeParser.parseDepth(minDepth, maxDepth, null);
    if (occurrenceParseResult.isSuccessful()) {
      lr.setDepth(occurrenceParseResult.getPayload().getValue());
      lr.setDepthAccuracy(occurrenceParseResult.getPayload().getAccuracy());
      occurrenceParseResult.getIssues().forEach(i -> addIssue(lr, i));
    }
  }

  /** {@link DwcTerm#minimumDistanceAboveSurfaceInMeters} interpretation. */
  public static void interpretMinimumDistanceAboveSurfaceInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.minimumDistanceAboveSurfaceInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMinimumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#maximumDistanceAboveSurfaceInMeters} interpretation. */
  public static void interpretMaximumDistanceAboveSurfaceInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.maximumDistanceAboveSurfaceInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMaximumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#coordinateUncertaintyInMeters} interpretation. */
  public static void interpretCoordinateUncertaintyInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.coordinateUncertaintyInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      Double result = parseResult.isSuccessful() ? Math.abs(parseResult.getPayload()) : null;
      if (result != null
          && result > COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND
          && result < COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND) {
        lr.setCoordinateUncertaintyInMeters(result);
      } else {
        addIssue(lr, COORDINATE_UNCERTAINTY_METERS_INVALID);
      }
    }
  }

  /** {@link DwcTerm#coordinatePrecision} interpretation. */
  public static void interpretCoordinatePrecision(ExtendedRecord er, LocationRecord lr) {

    Consumer<Optional<Double>> fn = parseResult -> {
      Double result = parseResult.orElse(null);
      if (result != null
          && result >= COORDINATE_PRECISION_LOWER_BOUND
          && result <= COORDINATE_PRECISION_UPPER_BOUND) {
        lr.setCoordinatePrecision(result);
      } else {
        addIssue(lr, COORDINATE_PRECISION_INVALID);
      }
    };

    SimpleTypeParser.parseDouble(er, DwcTerm.coordinatePrecision, fn);
  }

  /** {@link DwcTerm#locality} interpretation. */
  public static void interpretLocality(ExtendedRecord er, LocationRecord lr) {
    String value = extractNullAwareValue(er, DwcTerm.locality);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setLocality(cleanName(value));
    }
  }

  private static String cleanName(String x) {
    x = StringUtils.normalizeSpace(x).trim();
    // if we get all upper names, Capitalize them
    if (StringUtils.isAllUpperCase(StringUtils.deleteWhitespace(x))) {
      x = StringUtils.capitalize(x.toLowerCase());
    }
    return x;
  }
}
