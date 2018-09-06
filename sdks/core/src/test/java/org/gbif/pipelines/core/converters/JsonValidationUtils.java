package org.gbif.pipelines.core.converters;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonValidationUtils {

  private JsonValidationUtils() {}

  public static boolean isValid(String json) {
    try (JsonParser parser = new ObjectMapper().getFactory().createParser(json)) {
      while (parser.nextToken() != null) {
        // NOP
      }
    } catch (IOException ex) {
      return false;
    }
    return true;
  }
}
