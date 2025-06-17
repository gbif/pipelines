package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_PRECISION_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.FOOTPRINT_SRS_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.FOOTPRINT_WKT_MISMATCH;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import com.google.common.base.Strings;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DoubleAccuracy;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.interpreters.model.GadmFeatures;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.*;
import org.gbif.pipelines.core.interpreters.model.LocationRecord;
import org.gbif.pipelines.core.interpreters.model.MetadataRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/** Interprets the location terms of a {@link ExtendedRecord}. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocationInterpreter {

  // COORDINATE_UNCERTAINTY_METERS bounds are exclusive bounds
  private static final double COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND = 0d;
  // https://github.com/gbif/pipelines/issues/449
  private static final double COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND = 20_037_509d;

  private static final double COORDINATE_PRECISION_LOWER_BOUND = 0d;
  // 45 close to 5000 km
  private static final double COORDINATE_PRECISION_UPPER_BOUND = 1d;

  // List of Geospatial Issues
  private static final Set<String> SPATIAL_ISSUES =
      OccurrenceIssue.GEOSPATIAL_RULES.stream()
          .map(OccurrenceIssue::name)
          .collect(Collectors.toSet());

  private static final CountryParser COUNTRY_PARSER = CountryParser.getInstance();

  /** Determines if the record contains geospatial issues. */
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
      KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore, MetadataRecord mdr) {
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

        GeocodeRequest latLng = parsedLocation.getLatLng();
        if (Objects.nonNull(latLng)) {
          lr.setDecimalLatitude(latLng.getLat());
          lr.setDecimalLongitude(latLng.getLng());
          lr.setHasCoordinate(Boolean.TRUE);
        } else {
          lr.setHasCoordinate(Boolean.FALSE);
        }

        // set the issues to the interpretation
        lr.addIssue(parsedResult.getIssues());

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
   * Uses the interpreted {@link DwcTerm#decimalLatitude} and {@link DwcTerm#decimalLongitude} terms
   * to populate GADM administrative area GIDs.
   */
  public static BiConsumer<ExtendedRecord, LocationRecord> interpretGadm(
      KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore,
      Function<Void, GadmFeatures> createGadm
  ) {
    return (er, lr) -> {
      if (geocodeKvStore != null && lr.getHasCoordinate()) {
        GadmParser.parseGadm(lr, geocodeKvStore, createGadm).ifPresent(lr::setGadm);
      }
    };
  }

  /**
   * Uses the interpreted {@link DwcTerm#footprintSRS} and {@link DwcTerm#footprintWKT} terms to
   * populate the footprintWKT in WGS84 projection.
   */
  public static void interpretFootprintWKT(ExtendedRecord er, LocationRecord lr) {
    Optional<String> verbatimFootprintSRS = er.extractNullAwareOptValue(DwcTerm.footprintSRS);
    CoordinateReferenceSystem footprintSRS =
        verbatimFootprintSRS.map(SpatialReferenceSystemParser::parseCRS).orElse(null);

    if (verbatimFootprintSRS.isPresent() && footprintSRS == null) {
      lr.addIssue(FOOTPRINT_SRS_INVALID);
    } else {

      Optional<String> verbatimFootprintWKT = er.extractNullAwareOptValue(DwcTerm.footprintWKT);
      if (verbatimFootprintWKT.isPresent()) {
        // If the footprint is a POINT(lng lat), it was already used by the LocationParser.
        ParsedField<GeocodeRequest> parsedFootprint =
            CoordinateParseUtils.parsePointFootprintWKT(verbatimFootprintWKT.get());

        if (parsedFootprint.isSuccessful()) {
          // Check for conflict with the interpreted coordinates
          GeocodeRequest latLng = parsedFootprint.getResult();
          if (Math.abs(lr.getDecimalLatitude() - latLng.getLat()) <= 0.000001
              && Math.abs(lr.getDecimalLongitude() - latLng.getLng()) <= 0.000001) {
            // No conflict, but don't set the footprintWKT in the LocationRecord as it just
            // duplicates the coordinate.
            log.debug("duplicates the coordinate.");
          } else {
            lr.addIssue(FOOTPRINT_WKT_MISMATCH);
          }

        } else {
          // Footprint is not a valid POINT(lng lat).
          verbatimFootprintWKT
              .map(wkt -> FootprintWKTParser.parseFootprintWKT(footprintSRS, wkt))
              .ifPresent(
                  result -> {
                    if (result.isSuccessful()) {
                      lr.setFootprintWKT(result.getResult());
                    } else {
                      lr.addIssue(result.getIssues());
                    }
                  });
        }
      }
    }
  }

  /** Interprets the publishing country. */
  private static Optional<String> interpretPublishingCountry(ExtendedRecord er, MetadataRecord mr) {

    Optional<String> verbatimPublishingCountryCode =
            er.extractNullAwareOptValue(GbifTerm.publishingCountry);
    if (verbatimPublishingCountryCode.isPresent()) {
      OccurrenceParseResult<Country> result =
          new OccurrenceParseResult<>(COUNTRY_PARSER.parse(verbatimPublishingCountryCode.get()));

      if (result.isSuccessful()) {
        return Optional.of(result.getPayload().getIso2LetterCode());
      }
    }
    return Optional.ofNullable(mr.getDatasetPublishingCountry());
  }

  /** {@link DwcTerm#continent} interpretation. */
  public static BiConsumer<ExtendedRecord, LocationRecord> interpretContinent(
      KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore) {
    return (er, lr) -> {
      if (geocodeKvStore != null) {
        ParsedField<Continent> c = ContinentParser.parseContinent(er, lr, geocodeKvStore);
        if (c.isSuccessful()) {
          if (c.getResult() == null) {
            lr.setContinent(null); // Marine occurrence
          } else {
            lr.setContinent(c.getResult().name());
          }
          lr.getIssues().getIssueList().addAll(c.getIssues());
        }
      }
    };
  }

  /** {@link DwcTerm#waterBody} interpretation. */
  public static void interpretWaterBody(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.waterBody);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setWaterBody(cleanName(value));
    }
  }

  /** {@link DwcTerm#stateProvince} interpretation. */
  public static void interpretStateProvince(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.stateProvince);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setStateProvince(cleanName(value));
    }
  }

  /** {@link DwcTerm#minimumElevationInMeters} interpretation. */
  public static void interpretMinimumElevationInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.minimumElevationInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMinimumElevationInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#maximumElevationInMeters} interpretation. */
  public static void interpretMaximumElevationInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.maximumElevationInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMaximumElevationInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link GbifTerm#elevation} and {@link GbifTerm#elevationAccuracy} interpretation. */
  public static void interpretElevation(ExtendedRecord er, LocationRecord lr) {
    String minElevation = er.extractNullAwareValue(DwcTerm.minimumElevationInMeters);
    String maxElevation = er.extractNullAwareValue(DwcTerm.maximumElevationInMeters);
    OccurrenceParseResult<DoubleAccuracy> occurrenceParseResult =
        MeterRangeParser.parseElevation(minElevation, maxElevation, null);
    if (occurrenceParseResult.isSuccessful()) {
      lr.setElevation(occurrenceParseResult.getPayload().getValue());
      lr.setElevationAccuracy(occurrenceParseResult.getPayload().getAccuracy());
      occurrenceParseResult.getIssues().forEach(i -> lr.addIssue(i));
    }
  }

  /** {@link DwcTerm#minimumDepthInMeters} interpretation. */
  public static void interpretMinimumDepthInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.minimumDepthInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMinimumDepthInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#maximumDepthInMeters} interpretation. */
  public static void interpretMaximumDepthInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.maximumDepthInMeters);
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
    String minDepth = er.extractNullAwareValue(DwcTerm.minimumDepthInMeters);
    String maxDepth = er.extractNullAwareValue(DwcTerm.maximumDepthInMeters);
    OccurrenceParseResult<DoubleAccuracy> occurrenceParseResult =
        MeterRangeParser.parseDepth(minDepth, maxDepth, null);
    if (occurrenceParseResult.isSuccessful()) {
      lr.setDepth(occurrenceParseResult.getPayload().getValue());
      lr.setDepthAccuracy(occurrenceParseResult.getPayload().getAccuracy());
      occurrenceParseResult.getIssues().forEach(i -> lr.addIssue(i));
    }
  }

  /** {@link DwcTerm#minimumDistanceAboveSurfaceInMeters} interpretation. */
  public static void interpretMinimumDistanceAboveSurfaceInMeters(
          ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.minimumDistanceAboveSurfaceInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMinimumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#maximumDistanceAboveSurfaceInMeters} interpretation. */
  public static void interpretMaximumDistanceAboveSurfaceInMeters(
          ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.maximumDistanceAboveSurfaceInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      if (parseResult.isSuccessful()) {
        lr.setMaximumDistanceAboveSurfaceInMeters(parseResult.getPayload());
      }
    }
  }

  /** {@link DwcTerm#coordinateUncertaintyInMeters} interpretation. */
  public static void interpretCoordinateUncertaintyInMeters(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.coordinateUncertaintyInMeters);
    if (!Strings.isNullOrEmpty(value)) {
      ParseResult<Double> parseResult = MeterRangeParser.parseMeters(value);
      Double result = parseResult.isSuccessful() ? Math.abs(parseResult.getPayload()) : null;
      if (result != null
          && result > COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND
          && result < COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND) {
        lr.setCoordinateUncertaintyInMeters(result);
      } else {
        lr.addIssue(COORDINATE_UNCERTAINTY_METERS_INVALID);
      }
    }
  }

  /** {@link GbifTerm#distanceFromCentroidInMeters} interpretation. */
  public static BiConsumer<ExtendedRecord, LocationRecord> calculateCentroidDistance(
      KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore) {
    return (er, lr) -> {
      if (geocodeKvStore != null && lr.getHasCoordinate()) {
        CentroidCalculator.calculateCentroidDistance(lr, geocodeKvStore)
            .ifPresent(lr::setDistanceFromCentroidInMeters);
      }
    };
  }

  /** {@link DwcTerm#coordinatePrecision} interpretation. */
  public static void interpretCoordinatePrecision(ExtendedRecord er, LocationRecord lr) {

    Consumer<Optional<Double>> fn =
        parseResult -> {
          Double result = parseResult.orElse(null);
          if (result != null
              && result >= COORDINATE_PRECISION_LOWER_BOUND
              && result <= COORDINATE_PRECISION_UPPER_BOUND) {
            lr.setCoordinatePrecision(result);
          } else {
            lr.addIssue(COORDINATE_PRECISION_INVALID);
          }
        };

    SimpleTypeParser.parseDouble(er, DwcTerm.coordinatePrecision, fn);
  }

  /** {@link DwcTerm#locality} interpretation. */
  public static void interpretLocality(ExtendedRecord er, LocationRecord lr) {
    String value = er.extractNullAwareValue(DwcTerm.locality);
    if (!Strings.isNullOrEmpty(value)) {
      lr.setLocality(cleanName(value));
    }
  }

  /** {@link DwcTerm#higherGeography} interpretation. */
  public static void interpretHigherGeography(ExtendedRecord er, LocationRecord lr) {
    List<String> list = er.extractListValue(DwcTerm.higherGeography);
    if (!list.isEmpty()) {
      lr.setHigherGeography(list);
    }
  }

  /** {@link DwcTerm#locality} interpretation. */
  public static void interpretGeoreferencedBy(ExtendedRecord er, LocationRecord lr) {
    List<String> list = er.extractListValue(DwcTerm.georeferencedBy);
    if (!list.isEmpty()) {
      lr.setGeoreferencedBy(list);
    }
  }

  /** Use country to get GbifRegion */
  public static void interpretGbifRegion(LocationRecord lr) {
    setGbifRegion(lr.getCountryCode(), lr::setGbifRegion);
  }

  /** Use publishing country to get PublishedByGbifRegion */
  public static void interpretPublishedByGbifRegion(LocationRecord lr) {
    setGbifRegion(lr.getPublishingCountry(), lr::setPublishedByGbifRegion);
  }

//  /** Sets the coreId field. */
//  public static void setCoreId(ExtendedRecord er, LocationRecord lr) {
//    Optional.ofNullable(er.getCoreId()).ifPresent(lr::setCoreId);
//  }
//
//  /** Sets the parentEventId field. */
//  public static void setParentEventId(ExtendedRecord er, LocationRecord lr) {
//    extractOptValue(er, DwcTerm.parentEventID).ifPresent(lr::setParentId);
//  }

  private static void setGbifRegion(String countryIsoCode, Consumer<String> fn) {
    Optional.ofNullable(countryIsoCode)
        .map(Country::fromIsoCode)
        .map(Country::getGbifRegion)
        .map(GbifRegion::toString)
        .ifPresent(fn);
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
