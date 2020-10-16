package org.gbif.pipelines.core.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.io.IOException;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.io.avro.Multimedia;

/** Utility class to serialize and deserialize MediaObject instances from/to JSON. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MediaSerDeserUtils {

  abstract static class IgnoreSchemaProperty {

    // You have to use the correct package for JsonIgnore,
    // fasterxml or codehaus
    @JsonIgnore
    abstract void getSchema();
  }

  private static final String SER_ERROR_MSG = "Unable to serialize media objects to JSON";
  private static final String DESER_ERROR_MSG = "Unable to deserialize String into media objects";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // Don't change this section, methods used here guarantee backwards compatibility with Jackson
    // 1.8.8
    MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
    MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    MAPPER.addMixIn(Multimedia.class, IgnoreSchemaProperty.class);
  }

  private static final CollectionType LIST_MEDIA_TYPE =
      MAPPER.getTypeFactory().constructCollectionType(List.class, Multimedia.class);

  /** Converts the list of media objects into a JSON string. */
  @SneakyThrows
  public static String toJson(List<Multimedia> media) {
    try {
      if (media != null && !media.isEmpty()) {
        return MAPPER.writeValueAsString(media);
      }
    } catch (IOException ex) {
      log.error(SER_ERROR_MSG, ex);
      throw ex;
    }
    return null;
  }

  /** Converts a Json string into a list of media objects. */
  @SneakyThrows
  public static List<Multimedia> fromJson(String mediaJson) {
    try {
      return MAPPER.readValue(mediaJson, LIST_MEDIA_TYPE);
    } catch (IOException ex) {
      log.error(DESER_ERROR_MSG, ex);
      throw ex;
    }
  }
}
