package org.gbif.pipelines.core.factory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.avro.AvroModule;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SerDeFactory {

  private static final ObjectWriter AVRO_WRITER_NON_NULLS =
      new ObjectMapper()
          .registerModule(new AvroModule())
          .setSerializationInclusion(Include.NON_NULL)
          .writer();

  public static ObjectWriter avroWriterNonNulls() {
    return AVRO_WRITER_NON_NULLS;
  }
}
