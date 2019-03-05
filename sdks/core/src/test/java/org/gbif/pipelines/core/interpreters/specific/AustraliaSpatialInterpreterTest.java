package org.gbif.pipelines.core.interpreters.specific;

import java.io.IOException;
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
        return "{\"sname\":\"name,feature_co,record_id\",\"sid\":\"record_id\",\"defaultlayer\":true,\"id\":\"cl2123\","
            + "\"layerbranch\":false,\"last_update\":1472518286436,\"spid\":\"2123\",\"indb\":false,\"intersect\":false,"
            + "\"name\":\"Gazetteer of Australia 2012\",\"desc\":\"Gazetteer of Australia 2012\",\"type\":\"c\",\"analysis\":false,"
            + "\"addtomap\":false,\"sdesc\":\"state_id,status,latitude,longitude\",\"namesearch\":true,\"enabled\":true}";
      }

      @Override
      public void close() throws IOException {
      }
    };

    Map<String, String> resultMap = new HashMap<>();
    resultMap.put("layerbranch", "false");
    resultMap.put("sname", "name,feature_co,record_id");
    resultMap.put("sid", "record_id");
    resultMap.put("defaultlayer", "true");
    resultMap.put("id", "cl2123");
    resultMap.put("last_update", "1472518286436");
    resultMap.put("spid", "2123");
    resultMap.put("indb", "false");
    resultMap.put("intersect", "false");
    resultMap.put("name", "Gazetteer of Australia 2012");
    resultMap.put("desc", "Gazetteer of Australia 2012");
    resultMap.put("type", "c");
    resultMap.put("analysis", "false");
    resultMap.put("addtomap", "false");
    resultMap.put("sdesc", "state_id,status,latitude,longitude");
    resultMap.put("namesearch", "true");
    resultMap.put("enabled", "true");
    AustraliaSpatialRecord result = AustraliaSpatialRecord.newBuilder().setId("777").setItems(resultMap).build();

    // When
    AustraliaSpatialInterpreter.interpret(kvStore).accept(locationRecord, australiaSpatialRecord);

    // Should
    Assert.assertEquals(result, australiaSpatialRecord);
  }

}
