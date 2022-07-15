package org.gbif.pipelines.core.factory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SerDeFactory {

  private static final ObjectWriter OBJECT_WRITER_NON_NULLS =
      new ObjectMapper().setSerializationInclusion(Include.NON_NULL).writer();

  public static ObjectWriter writerNonNulls() {
    return OBJECT_WRITER_NON_NULLS;
  }
}
