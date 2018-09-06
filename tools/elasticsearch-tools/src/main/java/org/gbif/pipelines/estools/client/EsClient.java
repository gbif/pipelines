package org.gbif.pipelines.estools.client;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to communicate with the ES server.
 *
 * <p>It should be closed after using it. It implements {@link AutoCloseable}.
 */
public class EsClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EsClient.class);

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

    restClient = RestClient.builder(hosts).build();
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
    try {
      return restClient.performRequest(
          method, endpoint, params, body, createJsonContentTypeHeader());
    } catch (ResponseException exc) {
      throw exc;
    } catch (IOException exc) {
      LOG.error("Error when calling ES", exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  private Header[] createJsonContentTypeHeader() {
    return new Header[] {
      new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())
    };
  }

  @Override
  public void close() {
    try {
      restClient.close();
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }
}
