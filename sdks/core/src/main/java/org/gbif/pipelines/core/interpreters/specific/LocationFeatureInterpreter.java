package org.gbif.pipelines.core.interpreters.specific;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.interpreters.model.LocationFeatureRecord;
import org.gbif.pipelines.core.interpreters.model.LocationRecord;

/** Interprets the location of a {@link LocationFeatureRecord}. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocationFeatureInterpreter {

  public static BiConsumer<LocationRecord, LocationFeatureRecord> interpret(
      KeyValueStore<GeocodeRequest, String> kvStore) {
    return (lr, asr) -> {
      if (kvStore != null) {
        try {
          // Call kv store
          String json =
              kvStore.get(GeocodeRequest.create(lr.getDecimalLatitude(), lr.getDecimalLongitude()));

          // Parse json
          if (!Strings.isNullOrEmpty(json)) {
            json = json.substring(11, json.length() - 1);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> map =
                objectMapper.readValue(json, new TypeReference<HashMap<String, String>>() {});
            asr.setItems(map);
          }
        } catch (NoSuchElementException | NullPointerException | IOException ex) {
          log.error(ex.getMessage(), ex);
        }
      }
    };
  }
}
