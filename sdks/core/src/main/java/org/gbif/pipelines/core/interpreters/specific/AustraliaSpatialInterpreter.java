package org.gbif.pipelines.core.interpreters.specific;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Interprets the location of a {@link AustraliaSpatialRecord}. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AustraliaSpatialInterpreter {

  public static BiConsumer<LocationRecord, AustraliaSpatialRecord> interpret(KeyValueStore<LatLng, String> kvStore) {
    return (lr, asr) -> {
      if (kvStore != null) {
        try {
          // Call kv store
          String json = kvStore.get(new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude()));

          // Parse json
          if (!Strings.isNullOrEmpty(json)) {
            json = json.substring(11, json.length() - 1);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> map = objectMapper.readValue(json, new TypeReference<HashMap<String, String>>() {});
            asr.setItems(map);
          }
        } catch (NoSuchElementException | NullPointerException | IOException ex) {
          log.error(ex.getMessage(), ex);
        }
      }
    };
  }
}
