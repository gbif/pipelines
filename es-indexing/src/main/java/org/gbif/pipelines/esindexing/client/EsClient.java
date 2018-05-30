package org.gbif.pipelines.esindexing.client;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
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

public class EsClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EsClient.class);

  private final RestClient restClient;

  private EsClient(EsConfig config) {
    Objects.requireNonNull(config);
    Objects.requireNonNull(config.getHosts());
    Preconditions.checkArgument(!config.getHosts().isEmpty());

    HttpHost[] hosts = new HttpHost[config.getHosts().size()];
    for (int i = 0; i < config.getHosts().size(); i++) {
      EsConfig.EsHost esHost = config.getHosts().get(i);
      hosts[i] = new HttpHost(esHost.getHostname(), esHost.getPort(), esHost.getProtocol());
    }

    restClient = RestClient.builder(hosts).build();
  }

  public static EsClient from(EsConfig config) {
    return new EsClient(config);
  }

  public Response performGetRequest(String endpoint) throws ResponseException {
    return performGetRequest(endpoint, Collections.EMPTY_MAP, null);
  }

  public Response performGetRequest(String endpoint, Map<String, String> params, HttpEntity body)
    throws ResponseException {
    return performRequest(HttpGet.METHOD_NAME, endpoint, params, body);
  }

  public Response performPutRequest(String endpoint, Map<String, String> params, HttpEntity body)
    throws ResponseException {
    return performRequest(HttpPut.METHOD_NAME, endpoint, params, body);
  }

  public Response performPostRequest(String endpoint, Map<String, String> params, HttpEntity body)
    throws ResponseException {
    return performRequest(HttpPost.METHOD_NAME, endpoint, params, body);
  }

  private Response performRequest(String method, String endpoint, Map<String, String> params, HttpEntity body)
    throws ResponseException {
    try {
      return restClient.performRequest(method, endpoint, params, body, createJsonContentTypeHeader());
    } catch (ResponseException exc) {
      throw exc;
    } catch (IOException exc) {
      LOG.error("Error when calling ES", exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  private Header[] createJsonContentTypeHeader() {
    return new Header[] {new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())};
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
