package org.gbif.pipelines.util;

import java.io.IOException;
import java.util.Objects;

import com.cloudera.org.codehaus.jackson.JsonParser;
import com.cloudera.org.codehaus.jackson.map.ObjectMapper;

public class JsonValidationUtils {

  private JsonValidationUtils() {}

  public static boolean isValid(String json) {
    try (JsonParser parser = new ObjectMapper().getJsonFactory().createJsonParser(json)) {
      while (Objects.nonNull(parser.nextToken())) {
        // NOP
      }
    } catch (IOException ex) {
      return false;
    }
    return true;
  }
}
