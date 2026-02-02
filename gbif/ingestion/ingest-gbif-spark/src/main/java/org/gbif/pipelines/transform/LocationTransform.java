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
package org.gbif.pipelines.transform;

import java.io.Serializable;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transform.utils.GeocodeKVSFactory;
import org.gbif.rest.client.geocode.GeocodeResponse;

@Slf4j
public class LocationTransform implements Serializable {

  private final PipelinesConfig config;

  private LocationTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static LocationTransform create(PipelinesConfig config) {
    return new LocationTransform(config);
  }

  public LocationRecord convert(ExtendedRecord source, MetadataRecord mdr) throws Exception {
    if (source == null || source.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord is null or empty");
    }

    KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore =
        GeocodeKVSFactory.getKvStore(config);

    LocationRecord record =
        LocationRecord.newBuilder()
            .setId(source.getId())
            .setCoreId(source.getCoreId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    // Sequentially apply interpreters
    LocationInterpreter.interpretCountryAndCoordinates(geocodeKvStore, mdr).accept(source, record);
    LocationInterpreter.interpretContinent(geocodeKvStore).accept(source, record);
    LocationInterpreter.interpretGadm(geocodeKvStore).accept(source, record);
    LocationInterpreter.interpretWaterBody(source, record);
    LocationInterpreter.interpretStateProvince(source, record);
    LocationInterpreter.interpretMinimumElevationInMeters(source, record);
    LocationInterpreter.interpretMaximumElevationInMeters(source, record);
    LocationInterpreter.interpretElevation(source, record);
    LocationInterpreter.interpretMinimumDepthInMeters(source, record);
    LocationInterpreter.interpretMaximumDepthInMeters(source, record);
    LocationInterpreter.interpretDepth(source, record);
    LocationInterpreter.interpretMinimumDistanceAboveSurfaceInMeters(source, record);
    LocationInterpreter.interpretMaximumDistanceAboveSurfaceInMeters(source, record);
    LocationInterpreter.interpretCoordinatePrecision(source, record);
    LocationInterpreter.interpretCoordinateUncertaintyInMeters(source, record);
    LocationInterpreter.calculateCentroidDistance(geocodeKvStore).accept(source, record);
    LocationInterpreter.interpretLocality(source, record);
    LocationInterpreter.interpretFootprintWKT(source, record);
    LocationInterpreter.interpretHigherGeography(source, record);
    LocationInterpreter.interpretGeoreferencedBy(source, record);
    LocationInterpreter.interpretGbifRegion(record);
    LocationInterpreter.interpretPublishedByGbifRegion(record);
    LocationInterpreter.setCoreId(source, record);
    LocationInterpreter.setParentEventId(source, record);

    return record;
  }
}
