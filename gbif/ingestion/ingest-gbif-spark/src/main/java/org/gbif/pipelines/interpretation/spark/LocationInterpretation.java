/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.interpretation.spark;

import static org.gbif.dwc.terms.DwcTerm.GROUP_LOCATION;
import static org.gbif.dwc.terms.DwcTerm.parentEventID;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.transform.LocationTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

public class LocationInterpretation {

  /** Transforms the source records into the location records using the geocode service. */
  public static Dataset<LocationRecord> locationTransform(
      Config config, SparkSession spark, Dataset<ExtendedRecord> source) {
    LocationTransform locationTransform =
        LocationTransform.builder().geocodeApiUrl(config.getGeocodeAPI()).build();

    // extract the location
    Dataset<RecordWithLocation> recordWithLocation =
        source.map(
            (MapFunction<ExtendedRecord, RecordWithLocation>)
                er -> {
                  Location location = Location.buildFrom(er);
                  return RecordWithLocation.builder()
                      .id(er.getId())
                      .coreId(er.getCoreId())
                      .parentId(extractValue(er, parentEventID))
                      .locationHash(location.hash())
                      .location(location)
                      .build();
                },
            Encoders.bean(RecordWithLocation.class));
    recordWithLocation.createOrReplaceTempView("record_with_location");

    // distinct the locations to lookup
    Dataset<Location> distinctLocations =
        spark
            .sql("SELECT DISTINCT location.* FROM record_with_location")
            .repartition(config.getGeocodeParallelism())
            .as(Encoders.bean(Location.class));

    // lookup the distinct locations, and create a dictionary of the results
    Dataset<KeyedLocationRecord> keyedLocation =
        distinctLocations.map(
            (MapFunction<Location, KeyedLocationRecord>)
                location -> {

                  // HACK - the function takes ExtendedRecord, but we have a Location
                  ExtendedRecord er =
                      ExtendedRecord.newBuilder()
                          .setId("UNUSED_BUT_NECESSARY")
                          .setCoreTerms(location.toCoreTermsMap())
                          .build();

                  // look them up
                  Optional<LocationRecord> converted =
                      locationTransform.convert(
                          er, MetadataRecord.newBuilder().build()); // TODO MetadataRecord
                  if (converted.isPresent())
                    return KeyedLocationRecord.builder()
                        .key(location.hash())
                        .locationRecord(converted.get())
                        .build();
                  else
                    return KeyedLocationRecord.builder()
                        .key(location.hash())
                        .build(); // TODO: null handling?
                },
            Encoders.bean(KeyedLocationRecord.class));
    keyedLocation.createOrReplaceTempView("key_location");

    // join the dictionary back to the source records
    Dataset<RecordWithLocationRecord> expanded =
        spark
            .sql(
                "SELECT id, coreId, parentId, locationRecord "
                    + "FROM record_with_location r "
                    + "  LEFT JOIN key_location l ON r.locationHash = l.key")
            .as(Encoders.bean(RecordWithLocationRecord.class));

    return expanded.map(
        (MapFunction<RecordWithLocationRecord, LocationRecord>)
            r -> {
              LocationRecord locationRecord =
                  r.getLocationRecord() == null
                      ? LocationRecord.newBuilder().build()
                      : r.getLocationRecord();

              locationRecord.setId(r.getId());
              locationRecord.setCoreId(r.getCoreId());
              locationRecord.setParentId(r.getParentId());
              return locationRecord;
            },
        Encoders.bean(LocationRecord.class));
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithLocation {
    private String id;
    private String coreId;
    private String parentId;
    private String locationHash;
    private Location location;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class KeyedLocationRecord {
    private String key;
    private LocationRecord locationRecord;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithLocationRecord {
    private String id;
    private String coreId;
    private String parentId;
    private LocationRecord locationRecord;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Location {
    private String id;
    private String locationID;
    private String higherGeographyID;
    private String higherGeography;
    private String continent;
    private String waterBody;
    private String islandGroup;
    private String island;
    private String country;
    private String countryCode;
    private String stateProvince;
    private String county;
    private String municipality;
    private String locality;
    private String verbatimLocality;
    private String minimumElevationInMeters;
    private String maximumElevationInMeters;
    private String verbatimElevation;
    private String verticalDatum;
    private String minimumDepthInMeters;
    private String maximumDepthInMeters;
    private String verbatimDepth;
    private String minimumDistanceAboveSurfaceInMeters;
    private String maximumDistanceAboveSurfaceInMeters;
    private String locationAccordingTo;
    private String locationRemarks;
    private String decimalLatitude;
    private String decimalLongitude;
    private String geodeticDatum;
    private String coordinateUncertaintyInMeters;
    private String coordinatePrecision;
    private String pointRadiusSpatialFit;
    private String verbatimCoordinates;
    private String verbatimLatitude;
    private String verbatimLongitude;
    private String verbatimCoordinateSystem;
    private String verbatimSRS;
    private String footprintWKT;
    private String footprintSRS;
    private String footprintSpatialFit;
    private String georeferencedBy;
    private String georeferencedDate;
    private String georeferenceProtocol;
    private String georeferenceSources;
    private String georeferenceRemarks;

    static Location buildFrom(ExtendedRecord er) {
      LocationBuilder builder = Location.builder();

      Arrays.stream(DwcTerm.values())
          .filter(t -> GROUP_LOCATION.equals(t.getGroup()) && !t.isClass())
          .forEach(
              term -> {
                String fieldName = term.simpleName(); // e.g., "country"
                String value =
                    er.getCoreTerms()
                        .get(term.qualifiedName()); // or however the ER provides values

                if (value != null) {
                  try {
                    Method setter = builder.getClass().getMethod(fieldName, String.class);
                    setter.invoke(builder, value);
                  } catch (NoSuchMethodException e) {
                    System.err.println("No setter for: " + fieldName);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });

      return builder.build();
    }

    String hash() {
      return String.join(
          "|",
          id,
          locationID,
          higherGeographyID,
          higherGeography,
          continent,
          waterBody,
          islandGroup,
          island,
          country,
          countryCode,
          stateProvince,
          county,
          municipality,
          locality,
          verbatimLocality,
          minimumElevationInMeters,
          maximumElevationInMeters,
          verbatimElevation,
          verticalDatum,
          minimumDepthInMeters,
          maximumDepthInMeters,
          verbatimDepth,
          minimumDistanceAboveSurfaceInMeters,
          maximumDistanceAboveSurfaceInMeters,
          maximumDistanceAboveSurfaceInMeters,
          locationAccordingTo,
          locationRemarks,
          decimalLatitude,
          decimalLongitude,
          geodeticDatum,
          coordinateUncertaintyInMeters,
          coordinatePrecision,
          pointRadiusSpatialFit,
          verbatimCoordinates,
          verbatimLatitude,
          verbatimLongitude,
          verbatimCoordinateSystem,
          verbatimSRS,
          footprintWKT,
          footprintSRS,
          footprintSpatialFit,
          georeferencedBy,
          georeferencedDate,
          georeferenceProtocol,
          georeferenceSources,
          georeferenceRemarks);
    }

    public Map<String, String> toCoreTermsMap() {
      Map<String, String> coreTerms = new HashMap<>();

      BiConsumer<DwcTerm, String> ifNotNull =
          (term, value) -> {
            if (value != null) {
              coreTerms.put(term.qualifiedName(), value);
            }
          };

      ifNotNull.accept(DwcTerm.higherGeographyID, getHigherGeographyID());
      ifNotNull.accept(DwcTerm.higherGeography, getHigherGeography());
      ifNotNull.accept(DwcTerm.continent, getContinent());
      ifNotNull.accept(DwcTerm.waterBody, getWaterBody());
      ifNotNull.accept(DwcTerm.islandGroup, getIslandGroup());
      ifNotNull.accept(DwcTerm.island, getIsland());
      ifNotNull.accept(DwcTerm.country, getCountry());
      ifNotNull.accept(DwcTerm.countryCode, getCountryCode());
      ifNotNull.accept(DwcTerm.stateProvince, getStateProvince());
      ifNotNull.accept(DwcTerm.county, getCounty());
      ifNotNull.accept(DwcTerm.municipality, getMunicipality());
      ifNotNull.accept(DwcTerm.locality, getLocality());
      ifNotNull.accept(DwcTerm.verbatimLocality, getVerbatimLocality());
      ifNotNull.accept(DwcTerm.minimumElevationInMeters, getMinimumElevationInMeters());
      ifNotNull.accept(DwcTerm.maximumElevationInMeters, getMaximumElevationInMeters());
      ifNotNull.accept(DwcTerm.verbatimElevation, getVerbatimElevation());
      ifNotNull.accept(DwcTerm.verticalDatum, getVerticalDatum());
      ifNotNull.accept(DwcTerm.minimumDepthInMeters, getMinimumDepthInMeters());
      ifNotNull.accept(DwcTerm.maximumDepthInMeters, getMaximumDepthInMeters());
      ifNotNull.accept(DwcTerm.verbatimDepth, getVerbatimDepth());
      ifNotNull.accept(
          DwcTerm.minimumDistanceAboveSurfaceInMeters, getMinimumDistanceAboveSurfaceInMeters());
      ifNotNull.accept(
          DwcTerm.maximumDistanceAboveSurfaceInMeters, getMaximumDistanceAboveSurfaceInMeters());
      ifNotNull.accept(DwcTerm.locationAccordingTo, getLocationAccordingTo());
      ifNotNull.accept(DwcTerm.locationRemarks, getLocationRemarks());
      ifNotNull.accept(DwcTerm.decimalLatitude, getDecimalLatitude());
      ifNotNull.accept(DwcTerm.decimalLongitude, getDecimalLongitude());
      ifNotNull.accept(DwcTerm.geodeticDatum, getGeodeticDatum());
      ifNotNull.accept(DwcTerm.coordinateUncertaintyInMeters, getCoordinateUncertaintyInMeters());
      ifNotNull.accept(DwcTerm.coordinatePrecision, getCoordinatePrecision());
      ifNotNull.accept(DwcTerm.pointRadiusSpatialFit, getPointRadiusSpatialFit());
      ifNotNull.accept(DwcTerm.verbatimCoordinates, getVerbatimCoordinates());
      ifNotNull.accept(DwcTerm.verbatimLatitude, getVerbatimLatitude());
      ifNotNull.accept(DwcTerm.verbatimLongitude, getVerbatimLongitude());
      ifNotNull.accept(DwcTerm.verbatimCoordinateSystem, getVerbatimCoordinateSystem());
      ifNotNull.accept(DwcTerm.verbatimSRS, getVerbatimSRS());
      ifNotNull.accept(DwcTerm.footprintWKT, getFootprintWKT());
      ifNotNull.accept(DwcTerm.footprintSRS, getFootprintSRS());
      ifNotNull.accept(DwcTerm.footprintSpatialFit, getFootprintSpatialFit());
      ifNotNull.accept(DwcTerm.georeferencedBy, getGeoreferencedBy());
      ifNotNull.accept(DwcTerm.georeferencedDate, getGeoreferencedDate());
      ifNotNull.accept(DwcTerm.georeferenceProtocol, getGeoreferenceProtocol());
      ifNotNull.accept(DwcTerm.georeferenceSources, getGeoreferenceSources());
      ifNotNull.accept(DwcTerm.georeferenceRemarks, getGeoreferenceRemarks());

      return coreTerms;
    }
  }
}
