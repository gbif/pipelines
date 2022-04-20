package org.gbif.pipelines.core.converters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Utility class to validate Json objects. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class JsonValidationUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Is the string json parameter a valid JSON object. */
  static boolean isValid(String json) {
    try (JsonParser parser = OBJECT_MAPPER.getFactory().createParser(json)) {
      while (parser.nextToken() != null) {
        // NOP
      }
    } catch (IOException ex) {
      return false;
    }
    return true;
  }
}
