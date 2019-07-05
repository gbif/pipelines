package org.gbif.pipelines.ingest.hdfs.converters;

import org.gbif.pipelines.io.avro.Multimedia;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to serialize and deserialize MediaObject instances from/to JSON.
 */
public class MediaSerDeserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(org.gbif.occurrence.common.json.MediaSerDeserUtils.class);
  private static final String SER_ERROR_MSG = "Unable to serialize media objects to JSON";
  private static final String DESER_ERROR_MSG = "Unable to deserialize String into media objects";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  static {
    // Don't change this section, methods used here guarantee backwards compatibility with Jackson 1.8.8
    MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
    MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
  }

  private static final CollectionType LIST_MEDIA_TYPE = MAPPER.getTypeFactory().constructCollectionType(List.class,
                                                                                                        Multimedia.class);


  private MediaSerDeserUtils() {
    // private constructor
  }

  /**
   * Converts the list of media objects into a JSON string.
   */
  public static String toJson(List<Multimedia> media) {
    try {
      if (media != null && !media.isEmpty()) {
        return MAPPER.writeValueAsString(media);
      }
    } catch (IOException e) {
      logAndRethrow(SER_ERROR_MSG, e);
    }
    return null;
  }

  /**
   * Converts a Json string into a list of media objects.
   */
  public static List<Multimedia> fromJson(String mediaJson) {
    try {
      return MAPPER.readValue(mediaJson, LIST_MEDIA_TYPE);
    } catch (IOException e) {
      logAndRethrow(DESER_ERROR_MSG, e);
    }
    return null;
  }


  /**
   * Logs an error and re-throws the exception.
   */
  private static void logAndRethrow(String message, Throwable throwable) {
    LOG.error(message, throwable);
    Throwables.propagate(throwable);
  }


}
