package org.gbif.pipelines.transform;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.junit.Test;

public class LocationTransformTest {

  /** Ensure the kvs is only called with the same lat, lng, uncertainty to maximize cache usage */
  @Test
  public void testConvert() throws Exception {

    PipelinesConfig pipelinesConfig = new PipelinesConfig();

    AtomicInteger timesCalled = new AtomicInteger(0);
    final Set<GeocodeRequest> requests = new HashSet<>();

    final KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore =
        new KeyValueStore<>() {
          @Override
          public GeocodeResponse get(GeocodeRequest key) {
            requests.add(key);
            timesCalled.incrementAndGet();
            GeocodeResponse response = new GeocodeResponse();
            response.setLocations(
                java.util.List.of(
                    GeocodeResponse.Location.builder()
                        .id("1")
                        .name("Test Location")
                        .isoCountryCode2Digit("US")
                        .distance(10.0)
                        .build()));
            return response;
          }

          @Override
          public void close() {}
        };

    LocationTransform transform = LocationTransform.create(pipelinesConfig);

    ExtendedRecord source = new ExtendedRecord();
    source.setId("1");
    source.setCoreTerms(
        Map.of(
            DwcTerm.decimalLatitude.qualifiedName(), "10.0",
            DwcTerm.decimalLongitude.qualifiedName(), "20.0",
            DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), "30.0"));

    MetadataRecord mdr = new MetadataRecord();
    mdr.setId("1");

    transform.convert(source, mdr, geocodeKvStore);

    assertEquals(4, timesCalled.get());
    assertEquals(1, requests.size());

    assertEquals(10.0, requests.iterator().next().getLat(), 0.001);
    assertEquals(20.0, requests.iterator().next().getLng(), 0.001);
    assertEquals(30.0, requests.iterator().next().getUncertaintyMeters(), 0.001);
  }
}
