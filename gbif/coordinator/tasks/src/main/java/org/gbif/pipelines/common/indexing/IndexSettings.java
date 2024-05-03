package org.gbif.pipelines.common.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.gbif.pipelines.common.configs.IndexConfiguration;
import org.gbif.pipelines.tasks.occurrences.indexing.EsCatIndex;

@Slf4j
@Getter
public class IndexSettings {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final Integer numberOfShards;
  private final String indexName;

  private IndexSettings(
      IndexConfiguration indexConfig,
      HttpClient httpClient,
      String datasetId,
      Integer attempt,
      long recordsNumber)
      throws IOException {
    this.indexName = computeIndexName(indexConfig, httpClient, datasetId, attempt, recordsNumber);
    this.numberOfShards = computeNumberOfShards(indexConfig, indexName, recordsNumber);
  }

  private IndexSettings(String indexName, Integer numberOfShards) {
    this.numberOfShards = numberOfShards;
    this.indexName = indexName;
  }

  public static IndexSettings create(
      IndexConfiguration indexConfig,
      HttpClient httpClient,
      String datasetId,
      Integer attempt,
      long recordsNumber)
      throws IOException {
    return new IndexSettings(indexConfig, httpClient, datasetId, attempt, recordsNumber);
  }

  public static IndexSettings create(String indexName, Integer numberOfShards) {
    return new IndexSettings(indexName, numberOfShards);
  }

  /**
   * Computes the name for ES index:
   *
   * <pre>
   * Case 1 - Independent index for datasets where number of records more than config.indexIndepRecord
   * Case 2 - Default static index name for datasets where last changed date more than config.indexDefStaticDateDurationDd
   * Case 3 - Default dynamic index name for all other datasets
   * </pre>
   */
  private String computeIndexName(
      IndexConfiguration indexConfig,
      HttpClient httpClient,
      String datasetId,
      Integer attempt,
      long recordsNumber)
      throws IOException {

    // Independent index for datasets where number of records more than config.indexIndepRecord
    String idxName;

    if (recordsNumber >= indexConfig.bigIndexIfRecordsMoreThan) {
      idxName = datasetId + "_" + attempt + "_" + indexConfig.occurrenceVersion;
      idxName = idxName + "_" + Instant.now().toEpochMilli();
      log.info("ES Index name - {}, recordsNumber - {}", idxName, recordsNumber);
      return idxName;
    }

    // Default index name for all other datasets
    String esPr = indexConfig.defaultPrefixName + "_" + indexConfig.occurrenceVersion;
    idxName =
        getIndexName(indexConfig, httpClient, esPr)
            .orElse(esPr + "_" + Instant.now().toEpochMilli());
    log.info("ES Index name - {}", idxName);
    return idxName;
  }

  /**
   * Computes number of index shards:
   *
   * <pre>
   * 1) in case of default index -> config.indexDefSize / config.indexRecordsPerShard
   * 2) in case of independent index -> recordsNumber / config.indexRecordsPerShard
   * </pre>
   */
  private int computeNumberOfShards(
      IndexConfiguration indexConfig, String indexName, long recordsNumber) {
    if (indexName.startsWith(indexConfig.defaultPrefixName)) {
      int s =
          (int) Math.ceil((double) indexConfig.defaultSize / (double) indexConfig.recordsPerShard);

      // Add extra shard to accumulate deleted documents
      return indexConfig.defaultExtraShard ? s + 1 : s;
    }

    double shards = recordsNumber / (double) indexConfig.recordsPerShard;
    shards = Math.max(shards, 1d);
    boolean isCeil = (shards - Math.floor(shards)) > 0.25d;
    return isCeil ? (int) Math.ceil(shards) : (int) Math.floor(shards);
  }

  private Optional<String> getIndexName(
      IndexConfiguration indexConfig, HttpClient httpClient, String prefix) throws IOException {
    String url = String.format(indexConfig.defaultSmallestIndexCatUrl, prefix);
    HttpUriRequest httpGet = new HttpGet(url);
    HttpResponse response = httpClient.execute(httpGet);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException("ES _cat API exception " + response.getStatusLine().getReasonPhrase());
    }
    List<EsCatIndex> indices =
        MAPPER.readValue(
            response.getEntity().getContent(), new TypeReference<List<EsCatIndex>>() {});
    if (!indices.isEmpty() && indices.get(0).getCount() <= indexConfig.defaultNewIfSize) {
      return Optional.of(indices.get(0).getName());
    }
    return Optional.empty();
  }
}
