package org.gbif.pipelines.common.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.http.HttpResponse;
import org.gbif.pipelines.common.PipelinesException;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HttpUtils {

  /** This method is useful to do retries when a service is unavailable. */
  public static HttpResponse checkUnavailableService(HttpResponse response) {
    if (response.getStatusLine().getStatusCode() == 503
        || response.getStatusLine().getStatusCode() == 429) {
      throw new PipelinesException("Error: " + response.getStatusLine());
    }
    return response;
  }
}
