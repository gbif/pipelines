package org.gbif.pipelines.spark.util;

import feign.Headers;
import feign.Param;
import feign.RequestLine;
import java.util.UUID;
import org.gbif.validator.api.Validation;

public interface ValidationClient {

  @RequestLine("GET /validation/{key}")
  Validation get(@Param("key") UUID key);

  @RequestLine("PUT /validation/{key}")
  @Headers("Content-type: application/json")
  Validation update(@Param("key") UUID key, Validation validation);
}
