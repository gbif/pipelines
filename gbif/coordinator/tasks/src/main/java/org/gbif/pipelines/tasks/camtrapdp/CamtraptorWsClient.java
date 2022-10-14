package org.gbif.pipelines.tasks.camtrapdp;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/**
 * Simple client to perform requests to the Camtraptor server.
 */
@AllArgsConstructor
public class CamtraptorWsClient {

  private final String camtraptorWsUrl;

  /**
   * Converts a CamrtapDP package in the location identified by its datasetKey.
   * The dataset title is required by the Camtraptor to name the package file.
   */
  @SneakyThrows
  public void toDwca(UUID datasetKey, String datasetTitle) {
    URL url = buildUrl(datasetKey, datasetTitle);
    doRequest(url);
  }

  /**
   * Performs the GET request to the Camtraptor server.
   */
  @SneakyThrows
  private void doRequest(URL url) {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    if (HttpURLConnection.HTTP_OK != con.getResponseCode()) {
      throw new RuntimeException("Error contacting Camtraptor service " + con.getResponseMessage());
    }
    con.disconnect();
  }

  /**
   * Builds the target URL to the Camtraptor server.
   */
  @SneakyThrows
  private URL buildUrl(UUID datasetKey, String datasetTitle) {
    return  new URL(
      camtraptorWsUrl
      + "/to_dwca"
      + "?dataset_key="
      + datasetKey.toString()
      + "&dataset_title="
      + datasetTitle);
  }
}
