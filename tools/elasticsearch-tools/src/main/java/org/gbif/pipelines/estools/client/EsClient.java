package org.gbif.pipelines.estools.client;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

/**
 * Client to communicate with the ES server.
 *
 * <p>It should be closed after using it. It implements {@link AutoCloseable}.
 */
@Slf4j
public class EsClient implements AutoCloseable {

  private final RestClient restClient;

  private EsClient(EsConfig config) {
    Objects.requireNonNull(config);
    Objects.requireNonNull(config.getHosts());
    Preconditions.checkArgument(!config.getHosts().isEmpty());

    HttpHost[] hosts = new HttpHost[config.getHosts().size()];
    for (int i = 0; i < config.getHosts().size(); i++) {
      URL urlHost = config.getHosts().get(i);
      hosts[i] = new HttpHost(urlHost.getHost(), urlHost.getPort(), urlHost.getProtocol());
    }

    restClient = RestClient.builder(hosts).setMaxRetryTimeoutMillis(180_000).build();
  }

  /**
   * Creates a {@link EsClient} from a {@link EsConfig}
   *
   * @param config configuration with the ES hosts. It is required to specify at least 1 host.
   */
  public static EsClient from(EsConfig config) {
    return new EsClient(config);
  }

  /**
   * Performs a GET request.
   *
   * @param endpoint request's endpoint.
   * @return {@link Response}.
   * @throws ResponseException in case ES returns an error.
   */
  public Response performGetRequest(String endpoint) throws ResponseException {
    return performRequest(HttpGet.METHOD_NAME, endpoint, Collections.emptyMap(), null);
  }

  /**
   * Performs a PUT request.
   *
   * @param endpoint request's endpoint.
   * @param params query string parameters.
   * @param body request's body in JSON format.
   * @return {@link Response}.
   * @throws ResponseException in case ES returns an error.
   */
  public Response performPutRequest(String endpoint, Map<String, String> params, HttpEntity body)
      throws ResponseException {
    return performRequest(HttpPut.METHOD_NAME, endpoint, params, body);
  }

  /**
   * Performs a POST request.
   *
   * @param endpoint request's endpoint.
   * @param params query string parameters.
   * @param body request's body in JSON format.
   * @return {@link Response}.
   * @throws ResponseException in case ES returns an error.
   */
  public Response performPostRequest(String endpoint, Map<String, String> params, HttpEntity body)
      throws ResponseException {
    return performRequest(HttpPost.METHOD_NAME, endpoint, params, body);
  }

  /**
   * Performs a DELETE request.
   *
   * @param endpoint request's endpoint.
   * @return {@link Response}.
   * @throws ResponseException in case ES returns an error.
   */
  public Response performDeleteRequest(String endpoint) throws ResponseException {
    return performRequest(HttpDelete.METHOD_NAME, endpoint, Collections.emptyMap(), null);
  }

  private Response performRequest(
      String method, String endpoint, Map<String, String> params, HttpEntity body)
      throws ResponseException {
    // create request
    Request request = new Request(method, endpoint);
    request.setEntity(body);
    // add header to options
    RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
    optionsBuilder.addHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
    request.setOptions(optionsBuilder);
    // add parameters
    params.forEach(request::addParameter);
    try {
      return restClient.performRequest(request);
    } catch (ResponseException exc) {
      throw exc;
    } catch (IOException exc) {
      log.error("Error when calling ES", exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  @Override
  @SneakyThrows
  public void close() {
    restClient.close();
  }
}
