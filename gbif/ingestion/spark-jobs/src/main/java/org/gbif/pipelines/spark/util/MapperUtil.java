package org.gbif.pipelines.spark.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MapperUtil {

  public static final ObjectMapper MAPPER = new ObjectMapper();
  public static final ObjectMapper MAPPER_NON_EMPTY =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
}
