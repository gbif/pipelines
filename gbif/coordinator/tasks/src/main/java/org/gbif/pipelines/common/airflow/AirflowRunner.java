package org.gbif.pipelines.common.airflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

@Getter
@Setter
@Slf4j
public class AirflowRunner {
  private CloseableHttpClient client;
  private ArrayList<NameValuePair> headers;
  private String dagId;
  private String dagRunId;
  private String baseurl;
  private AirflowBody body;

  ObjectMapper mapper = new ObjectMapper();

  public CloseableHttpResponse createRun() {
    HttpPost post;
    try {
      client = HttpClients.createDefault();
      post = new HttpPost(baseurl + "/dags/" + dagId + "/dagRuns");
      StringEntity input =
          new StringEntity(
              "{\"dag_run_id\": \""
                  + dagRunId
                  + "\", \"conf\": "
                  + mapper.writeValueAsString(body)
                  + "}");
      input.setContentType(ContentType.APPLICATION_JSON.toString());
      post.setEntity(input);
      for (NameValuePair pair : headers) {
        post.addHeader(pair.getName(), pair.getValue());
      }
      return client.execute(post);
    } catch (UnsupportedEncodingException e) {
      log.error("Unable to encode the request", e);
      throw new RuntimeException(e);
    } catch (JsonProcessingException e) {
      log.error("Unable to process the body of airflow request", e);
      throw new RuntimeException(e);
    } catch (ClientProtocolException e) {
      log.error("Error calling with the client.", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      log.error("Error during communication with airflow.", e);
      throw new RuntimeException(e);
    } finally {
      try {
        client.close();
      } catch (IOException e) {
        log.error("Error closing the client.", e);
      }
    }
  }
}
