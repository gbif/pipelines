package org.gbif.pipelines.spark.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.gbif.pipelines.core.config.model.IndexConfig;
import org.junit.Test;

public class IndexSettingsTest {

  @Test
  public void testGetExistingDefaultIndexName_found() throws Exception {
    IndexConfig config = new IndexConfig();
    config.defaultIndexCatUrl = "http://es-host:9200";
    config.defaultNewIfSize = 1000; // big threshold to allow match

    String defaultPrefix = config.defaultPrefixName + "_occurrence_a";
    String matchingIndex = defaultPrefix + "_" + System.currentTimeMillis();

    // alias API returns a list of objects that contain the index name
    String aliasJson = "[{\"index\": \"" + matchingIndex + "\"}]";

    // indices API returns docs.count and index fields
    String indicesJson = "[{\"docs.count\": 10, \"index\": \"" + matchingIndex + "\"}]";

    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse respAlias = mock(HttpResponse.class);
    HttpResponse respIndices = mock(HttpResponse.class);
    StatusLine okStatus = mock(StatusLine.class);
    HttpEntity entityAlias = mock(HttpEntity.class);
    HttpEntity entityIndices = mock(HttpEntity.class);

    when(okStatus.getStatusCode()).thenReturn(200);

    when(respAlias.getStatusLine()).thenReturn(okStatus);
    when(respIndices.getStatusLine()).thenReturn(okStatus);

    when(entityAlias.getContent())
        .thenReturn(new ByteArrayInputStream(aliasJson.getBytes(StandardCharsets.UTF_8)));
    when(entityIndices.getContent())
        .thenReturn(new ByteArrayInputStream(indicesJson.getBytes(StandardCharsets.UTF_8)));

    when(respAlias.getEntity()).thenReturn(entityAlias);
    when(respIndices.getEntity()).thenReturn(entityIndices);

    // httpClient.execute will be called twice: first for alias, second for indices
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(respAlias, respIndices);

    Optional<String> res =
        IndexSettings.getExistingDefaultIndexName(config, httpClient, "occurrence", defaultPrefix);

    assertThat(res).isPresent();
    assertThat(res.get()).isEqualTo(matchingIndex);
  }

  @Test
  public void testGetExistingDefaultIndexName_notFound() throws Exception {
    IndexConfig config = new IndexConfig();
    config.defaultIndexCatUrl = "http://es-host:9200";
    config.defaultNewIfSize = 5; // small threshold so counts will be too big

    String defaultPrefix = config.defaultPrefixName + "_occurrence_a";
    String matchingIndex = defaultPrefix + "_" + System.currentTimeMillis();

    String aliasJson = "[{\"index\": \"" + matchingIndex + "\"}]";
    // docs.count is above the threshold -> should not be selected
    String indicesJson = "[{\"docs.count\": 100, \"index\": \"" + matchingIndex + "\"}]";

    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse respAlias = mock(HttpResponse.class);
    HttpResponse respIndices = mock(HttpResponse.class);
    StatusLine okStatus = mock(StatusLine.class);
    HttpEntity entityAlias = mock(HttpEntity.class);
    HttpEntity entityIndices = mock(HttpEntity.class);

    when(okStatus.getStatusCode()).thenReturn(200);

    when(respAlias.getStatusLine()).thenReturn(okStatus);
    when(respIndices.getStatusLine()).thenReturn(okStatus);

    when(entityAlias.getContent())
        .thenReturn(new ByteArrayInputStream(aliasJson.getBytes(StandardCharsets.UTF_8)));
    when(entityIndices.getContent())
        .thenReturn(new ByteArrayInputStream(indicesJson.getBytes(StandardCharsets.UTF_8)));

    when(respAlias.getEntity()).thenReturn(entityAlias);
    when(respIndices.getEntity()).thenReturn(entityIndices);

    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(respAlias, respIndices);

    Optional<String> res =
        IndexSettings.getExistingDefaultIndexName(config, httpClient, "alias", defaultPrefix);

    assertThat(res).isEmpty();
  }
}
