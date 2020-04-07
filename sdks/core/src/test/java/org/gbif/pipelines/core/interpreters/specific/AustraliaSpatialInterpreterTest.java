package org.gbif.pipelines.core.interpreters.specific;

import java.util.HashMap;
import java.util.Map;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;

import org.junit.Assert;
import org.junit.Test;

public class AustraliaSpatialInterpreterTest {

  @Test
  public void australiaSpatialInterpreterTest() {

    // State
    LocationRecord locationRecord = LocationRecord.newBuilder()
        .setId("777")
        .build();

    AustraliaSpatialRecord australiaSpatialRecord = AustraliaSpatialRecord.newBuilder().setId("777").build();

    KeyValueStore<LatLng, String> kvStore = new KeyValueStore<LatLng, String>() {
      @Override
      public String get(LatLng latLng) {
        return "{\"layers: \"{\"cb1\":\"1\",\"cb2\":\"2\",\"cb3\":\"3\"}}";
      }

      @Override
      public void close() {
        // NOP
      }
    };

    Map<String, String> resultMap = new HashMap<>();
    resultMap.put("cb1", "1");
    resultMap.put("cb2", "2");
    resultMap.put("cb3", "3");
    AustraliaSpatialRecord result = AustraliaSpatialRecord.newBuilder().setId("777").setItems(resultMap).build();

    // When
    AustraliaSpatialInterpreter.interpret(kvStore).accept(locationRecord, australiaSpatialRecord);

    // Should
    Assert.assertEquals(result, australiaSpatialRecord);
  }

}
