/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.airflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.AirflowConfig;

@Slf4j
@Builder
public class AirflowClient {

  private static final String DAG_RUN_ID = "dag_run_id";

  private final AirflowConfig config;

  private final String dagName;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String getUri(AirflowConfig config) {
    String uri = String.join("/", config.getAddress(), "dags", dagName, "dagRuns");
    log.debug("Get Airflow Run ID: {}", uri);
    return uri;
  }

  private String getUri(AirflowConfig config, String paths) {
    String uri = String.join("/", getUri(config), paths);
    log.debug("Get Airflow Run ID with path: {}", uri);
    return uri;
  }

  @SneakyThrows
  public JsonNode createRun(AirflowBody body) {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      JsonNode dagRun = getRun(body.getDagRunId());

      // Delete dag_run_id to avoid issues with params cache
      if (dagRun.has(DAG_RUN_ID) && dagRun.get(DAG_RUN_ID).asText().equals(body.getDagRunId())) {
        log.debug(
            "dag_run_id {} exists. Deleting the run to avoid caching issues", body.getDagRunId());
        HttpDelete delete = new HttpDelete(getUri(config, body.getDagRunId()));
        delete.setHeaders(getHeaders(config));
        checkUnavailableService(client.execute(delete));
      }

      log.debug("Submit dag_run_id {}", body.getDagRunId());
      HttpPost post = new HttpPost(getUri(config));
      StringEntity input = new StringEntity(MAPPER.writeValueAsString(body));
      input.setContentType(ContentType.APPLICATION_JSON.toString());
      post.setEntity(input);
      post.setHeaders(getHeaders(config));

      HttpResponse response = checkUnavailableService(client.execute(post));
      log.debug(
          "Submit dag_run_id {} response code {}, reason {}",
          body.getDagRunId(),
          response.getStatusLine().getStatusCode(),
          response.getStatusLine().getReasonPhrase());

      return MAPPER.readTree(response.getEntity().getContent());
    }
  }

  @SneakyThrows
  public JsonNode getRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpGet get = new HttpGet(getUri(config, dagRunId));
      get.setHeaders(getHeaders(config));
      return MAPPER.readTree(checkUnavailableService(client.execute(get)).getEntity().getContent());
    }
  }

  /** This method is useful to do retries when a service is unavailable. */
  public static HttpResponse checkUnavailableService(HttpResponse response) {
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE
        || response.getStatusLine().getStatusCode() == 429) {
      throw new PipelinesException("Error: " + response.getStatusLine());
    }
    return response;
  }

  @JsonIgnore
  public String getBasicAuthString(AirflowConfig config) {
    String stringToEncode = config.getUser() + ":" + config.getPass();
    return Base64.getEncoder().encodeToString(stringToEncode.getBytes(StandardCharsets.UTF_8));
  }

  public Header[] getHeaders(AirflowConfig config) {
    return new Header[] {
      new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + getBasicAuthString(config)),
      new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())
    };
  }
}
