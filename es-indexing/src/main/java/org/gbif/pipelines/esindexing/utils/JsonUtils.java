package org.gbif.pipelines.esindexing.utils;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;

public class JsonUtils {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
  private static final ObjectReader reader = mapper.readerFor(Map.class);

  public static ObjectNode createObjectNode() {
    return mapper.createObjectNode();
  }

  public static ArrayNode createArrayNode() {
    return mapper.createArrayNode();
  }

  public static String writeJsonToString(Object obj) {
    try {
      return writer.writeValueAsString(obj);
    } catch (JsonProcessingException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  public static Map<String, String> readEntity(HttpEntity entity) {
    try {
      return reader.readValue(entity.getContent());
    } catch (IOException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

}
