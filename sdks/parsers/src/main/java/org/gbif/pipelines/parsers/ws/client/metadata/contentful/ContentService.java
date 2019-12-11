package org.gbif.pipelines.parsers.ws.client.metadata.contentful;

import org.gbif.pipelines.parsers.ws.client.metadata.response.Programme;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Project;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Client service to Elastisarch/Contentful CMS service.
 */
public class ContentService {

  private RestHighLevelClient restHighLevelClient;
  private static final String DEFAULT_LOCALE = "en-GB";

  private static RestHighLevelClient buildClient(String...hostsAddresses) {
    HttpHost[] hosts =
      Arrays.stream(hostsAddresses)
        .map(
          address -> {
            try {
              URL urlHost =  new URL(address);
              return new HttpHost(urlHost.getHost(), urlHost.getPort(), urlHost.getProtocol());
            } catch (MalformedURLException e) {
              throw new IllegalArgumentException(address + " is not a valid url", e);
            }
          }).toArray(HttpHost[]::new);
    RestClientBuilder builder = RestClient.builder(hosts).setMaxRetryTimeoutMillis(180_000);
    return new RestHighLevelClient(builder);
  }

  private String getFieldValue(Map<String, Object> source, String...field) {
    Object value  = source.get(field[0]);
    if (Objects.nonNull(value)) {
      if (value instanceof Map) {
         Map<String,Object> valueMap  = (Map<String, Object>)value;
         return getFieldValue(valueMap, Arrays.copyOfRange(field,1, field.length));
      } else {
        return value.toString();
      }
    }
    return null;
  }

  /**
   *
   * @param hosts Elasticsearch hosts
   */
  public ContentService(String...hosts) {
    restHighLevelClient = buildClient(hosts);
  }

  /**
   * Gets a project by its projectId field in Contentful.
   * @param projectId to be queried
   * @return a project linked to the identifier, null otherwise
   */
  public Project getProject(String projectId) {
    try {
      SearchSourceBuilder searchSourceBuilder =
        new SearchSourceBuilder().query(QueryBuilders.termQuery("projectId", projectId)).size(1);
      SearchRequest searchRequest = new SearchRequest().indices("project").source(searchSourceBuilder);

      SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
      if (response.getHits().getTotalHits() > 0) {
        Map<String,Object> sourceFields = response.getHits().getHits()[0].getSourceAsMap();
        return new Project(getFieldValue(sourceFields,"title", DEFAULT_LOCALE),
                           getFieldValue(sourceFields, "projectId"),
                           getProgramme(getFieldValue(sourceFields, "programme", "id")));
      }
      return null;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Converts a project entry/resource into a Programme object.
   * Returns null if the project doesn't have an associated programme.*/
  private Programme getProgramme(String programmeId) {
    try {
      if (Objects.nonNull(programmeId)) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.idsQuery()
                                                                                    .addIds(programmeId)).size(1);
        SearchRequest searchRequest = new SearchRequest().indices("programme").source(searchSourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        if (response.getHits().getTotalHits() > 0) {
          Map<String,Object> sourceFields = response.getHits().getHits()[0].getSourceAsMap();
          return new Programme(getFieldValue(sourceFields,"id"),
                               getFieldValue(sourceFields,"title", DEFAULT_LOCALE),
                               getFieldValue(sourceFields,"acronym"));
        }
      }
      return null;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

 }
