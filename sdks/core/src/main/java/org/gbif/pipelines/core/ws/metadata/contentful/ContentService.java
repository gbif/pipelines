package org.gbif.pipelines.core.ws.metadata.contentful;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.gbif.pipelines.core.ws.metadata.response.Programme;
import org.gbif.pipelines.core.ws.metadata.response.Project;

/** Client service to Elastisarch/Contentful CMS service. */
public class ContentService {

  private final ElasticsearchClient elasticsearchClient;
  private static final String DEFAULT_LOCALE = "en-GB";

  private static ElasticsearchClient buildClient(String... hostsAddresses) {
    HttpHost[] hosts =
        Arrays.stream(hostsAddresses)
            .map(
                address -> {
                  try {
                    URL urlHost = new URL(address);
                    return new HttpHost(
                        urlHost.getHost(), urlHost.getPort(), urlHost.getProtocol());
                  } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(address + " is not a valid url", e);
                  }
                })
            .toArray(HttpHost[]::new);
    RestClient restClient =
        RestClient.builder(hosts)
            .setRequestConfigCallback(b -> b.setConnectTimeout(180_000).setSocketTimeout(180_000))
            .build();
    return new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
  }

  /**
   * @param hosts Elasticsearch hosts
   */
  public ContentService(String... hosts) {
    elasticsearchClient = buildClient(hosts);
  }

  /** Release ES content client */
  @SneakyThrows
  public void close() {
    elasticsearchClient.close();
  }

  /**
   * Gets a project by its projectId field in Contentful.
   *
   * @param projectId to be queried
   * @return a project linked to the identifier, null otherwise
   */
  @SneakyThrows
  public Project getProject(String projectId) {
    SearchResponse<Map> response =
        elasticsearchClient.search(
            s ->
                s.index("project")
                    .size(1)
                    .query(q -> q.term(t -> t.field("projectId").value(projectId))),
            Map.class);
    if (hasHits(response)) {
      Map<String, Object> sourceFields = response.hits().hits().get(0).source();
      return new Project(
          getFieldValue(sourceFields, "title", DEFAULT_LOCALE),
          getFieldValue(sourceFields, "projectId"),
          getProgramme(getFieldValue(sourceFields, "programme", "id")));
    }
    return null;
  }

  /**
   * Converts a project entry/resource into a Programme object. Returns null if the project doesn't
   * have an associated programme.
   */
  @SneakyThrows
  private Programme getProgramme(String programmeId) {
    if (Objects.nonNull(programmeId)) {
      SearchResponse<Map> response =
          elasticsearchClient.search(
              s -> s.index("programme").size(1).query(q -> q.ids(i -> i.values(programmeId))),
              Map.class);
      if (hasHits(response)) {
        Map<String, Object> sourceFields = response.hits().hits().get(0).source();
        return new Programme(
            getFieldValue(sourceFields, "id"),
            getFieldValue(sourceFields, "title", DEFAULT_LOCALE),
            getFieldValue(sourceFields, "acronym"));
      }
    }
    return null;
  }

  private static boolean hasHits(SearchResponse<Map> response) {
    return !response.hits().hits().isEmpty()
        || (response.hits().total() != null && response.hits().total().value() > 0);
  }

  private String getFieldValue(Map<String, Object> source, String... field) {
    Object value = source.get(field[0]);
    if (Objects.nonNull(value)) {
      if (value instanceof Map) {
        Map<String, Object> valueMap = (Map<String, Object>) value;
        return getFieldValue(valueMap, Arrays.copyOfRange(field, 1, field.length));
      } else {
        return value.toString();
      }
    }
    return null;
  }
}
