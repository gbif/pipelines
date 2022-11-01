package org.gbif.pipelines.tasks.camtrapdp;

import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple client to perform requests to the Camtraptor server.
 *
 * @see <a href="https://github.com/inbo/camtraptor/">Camtraptor</a> for more detail.
 */
@AllArgsConstructor
@Slf4j
public class CamtraptorWsClient {

  private static final Retry RETRY =
      Retry.of(
          "camtraptorCall",
          RetryConfig.custom()
              .maxAttempts(3)
              .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(2)))
              .build());

  private final String camtraptorWsUrl;

  /**
   * Converts a CamrtapDP package in the location identified by its datasetKey. The dataset title is
   * required by the Camtraptor to name the package file.
   */
  public void toDwca(UUID datasetKey, String datasetTitle) {
    URL url = buildUrl(datasetKey, datasetTitle);
    log.info("Querying Camtraptor server " + url);
    Retry.decorateRunnable(RETRY, () -> doRequest(url)).run();
  }

  /** Performs the GET request to the Camtraptor server. */
  @SneakyThrows
  private void doRequest(URL url) {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    if (HttpURLConnection.HTTP_OK != con.getResponseCode()) {
      throw new RuntimeException("Error contacting Camtraptor service " + con.getResponseMessage());
    }
    con.disconnect();
  }

  /** Builds the target URL to the Camtraptor server. */
  @SneakyThrows
  private URL buildUrl(UUID datasetKey, String datasetTitle) {
    return new URL(
        camtraptorWsUrl
            + "/to_dwca"
            + "?dataset_key="
            + datasetKey.toString()
            + "&dataset_title="
            + URLEncoder.encode(datasetTitle, StandardCharsets.UTF_8.name()));
  }
}
