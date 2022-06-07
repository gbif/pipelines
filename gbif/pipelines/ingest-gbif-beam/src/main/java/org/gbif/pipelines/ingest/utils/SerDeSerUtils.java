package org.gbif.pipelines.ingest.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.gbif.pipelines.io.avro.json.GbifClassification;

/** Utility classes and methods to serialize JSon test data. */
@UtilityClass
public class SerDeSerUtils {

  /** Mixin to map class JSon attribute to class$ java property. */
  public abstract static class GbifClassificationMixin {

    @JsonProperty("class")
    public abstract String getClass$();
  }

  /** Creates a Jackson Mapper with Mixins used to convert Elasticsearch response. */
  public static ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.addMixIn(GbifClassification.class, GbifClassificationMixin.class);
    return mapper;
  }
}
