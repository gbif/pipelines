package org.gbif.pipelines.core.factory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroModule;
import lombok.experimental.UtilityClass;
import org.gbif.pipelines.io.avro.json.GbifClassification;

@UtilityClass
public class SerDeFactory {

  private static final ObjectMapper AVRO_MAPPER_NON_NULLS =
      new ObjectMapper()
          .addMixIn(GbifClassification.class, GbifClassificationMixin.class)
          .registerModule(new AvroModule())
          .setSerializationInclusion(Include.NON_EMPTY);

  public static ObjectMapper avroMapperNonNulls() {
    return AVRO_MAPPER_NON_NULLS;
  }

  public abstract class GbifClassificationMixin {

    @JsonProperty("class")
    public abstract String getClass$();
  }
}
