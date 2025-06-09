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
package org.gbif.pipelines.interpretation.transform;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.interpretation.transform.utils.GeocodeKVSFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;

@Slf4j
@Builder
public class LocationTransform implements Serializable {
  private String geocodeApiUrl;

  public Optional<LocationRecord> convert(ExtendedRecord source, MetadataRecord mdr) {

    KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore =
        GeocodeKVSFactory.getKvStore(geocodeApiUrl);

    Interpretation<ExtendedRecord>.Handler<LocationRecord> handler =
        Interpretation.from(source)
            .to(
                LocationRecord.newBuilder()
                    .setId(source.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
            .when(er -> !er.getCoreTerms().isEmpty())
            .via(LocationInterpreter.interpretCountryAndCoordinates(geocodeKvStore, mdr))
            .via(LocationInterpreter.interpretContinent(geocodeKvStore))
            .via(LocationInterpreter.interpretGadm(geocodeKvStore))
            .via(LocationInterpreter::interpretWaterBody)
            .via(LocationInterpreter::interpretStateProvince)
            .via(LocationInterpreter::interpretMinimumElevationInMeters)
            .via(LocationInterpreter::interpretMaximumElevationInMeters)
            .via(LocationInterpreter::interpretElevation)
            .via(LocationInterpreter::interpretMinimumDepthInMeters)
            .via(LocationInterpreter::interpretMaximumDepthInMeters)
            .via(LocationInterpreter::interpretDepth)
            .via(LocationInterpreter::interpretMinimumDistanceAboveSurfaceInMeters)
            .via(LocationInterpreter::interpretMaximumDistanceAboveSurfaceInMeters)
            .via(LocationInterpreter::interpretCoordinatePrecision)
            .via(LocationInterpreter::interpretCoordinateUncertaintyInMeters)
            .via(LocationInterpreter.calculateCentroidDistance(geocodeKvStore))
            .via(LocationInterpreter::interpretLocality)
            .via(LocationInterpreter::interpretFootprintWKT)
            .via(LocationInterpreter::interpretHigherGeography)
            .via(LocationInterpreter::interpretGeoreferencedBy)
            .via(LocationInterpreter::interpretGbifRegion)
            .via(LocationInterpreter::interpretPublishedByGbifRegion)
            .via(LocationInterpreter::setCoreId)
            .via(LocationInterpreter::setParentEventId);
    return handler.getOfNullable();
  }
}
