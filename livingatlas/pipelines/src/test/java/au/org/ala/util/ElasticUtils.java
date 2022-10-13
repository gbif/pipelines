package au.org.ala.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class ElasticUtils {

  public static Long getRecordCount(String term, String value) throws Exception {
    RestHighLevelClient client = getRestHighLevelClient();
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery(term, value));
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    return searchResponse.getHits().getTotalHits().value;
  }

  public static Long getRecordCount() throws Exception {
    RestHighLevelClient client = getRestHighLevelClient();

    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    return searchResponse.getHits().getTotalHits().value;
  }

  @NotNull
  private static RestHighLevelClient getRestHighLevelClient() {
    RestHighLevelClient client =
        new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(
                    "localhost", Integer.parseInt(System.getProperty("ES_PORT")), "http")));
    return client;
  }

  public static void refreshIndex() throws Exception {
    String url = "http://localhost:" + System.getProperty("ES_PORT") + "/event/_refresh";
    JsonNode node = new ObjectMapper().readTree(new URL(url));
  }
}
