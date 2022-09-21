package org.gbif.pipelines.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.gbif.pipelines.common.configs.RegistryConfiguration;
import org.gbif.pipelines.tasks.MachineTag;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GbifApi {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Get number of record using Occurrence API */
  @SneakyThrows
  public static long getIndexSize(
      HttpClient httpClient, RegistryConfiguration config, String datasetId) {
    int nano = LocalDateTime.now().getNano();
    String url = config.wsUrl + "/occurrence/search?limit=0&datasetKey=" + datasetId + "&_" + nano;
    HttpResponse response = executeGet(httpClient, url);
    return MAPPER.readTree(response.getEntity().getContent()).findValue("count").asLong();
  }

  @SneakyThrows
  public static Optional<String> getMachineTagValue(
      HttpClient httpClient, RegistryConfiguration config, String datasetId, String tagName) {
    String url = config.wsUrl + "/dataset/" + datasetId + "/machineTag";
    HttpResponse response = executeGet(httpClient, url);

    List<MachineTag> machineTags =
        MAPPER.readValue(
            response.getEntity().getContent(), new TypeReference<List<MachineTag>>() {});

    return machineTags.stream()
        .filter(x -> x.getName().equals(tagName))
        .map(MachineTag::getValue)
        .findFirst();
  }

  @SneakyThrows
  private static HttpResponse executeGet(HttpClient httpClient, String url) {
    HttpResponse response = httpClient.execute(new HttpGet(url));
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new PipelinesException(
          "GBIF API exception " + response.getStatusLine().getReasonPhrase());
    }
    return response;
  }
}
