package org.gbif.pipelines.ingest.pipelines.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.elasticsearch.client.Response;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsTestUtils {

  // datasets to test
  public static final String DATASET_TEST = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
  public static final String DATASET_TEST_2 = "96c7660c-1356-4732-8a7c-0b3eeb20fb27";
  public static final String DATASET_TEST_3 = "faf8b3c3-21dc-48b6-8a5c-e1f037789f74";
  public static final String DATASET_TEST_4 = "a180b0b4-f527-4da5-81fb-ee085e3ce4f4";
  public static final String DATASET_TEST_5 = "197908d0-5565-11d8-b290-b8a03c50a862";
  public static final String DATASET_TEST_6 = "b5441d39-7564-4c70-bd9d-b90236808892";
  public static final String DATASET_TEST_7 = "902c8fe7-8f38-45b0-854e-c324fed36303";
  public static final String DATASET_TEST_8 = "7e3ec3b3-de71-4389-b3ea-71d0cae64631";
  public static final String DATASET_TEST_9 = "758478a0-f762-11e1-a439-00145eb45e9a";

  public static final String ALIAS = "alias";
  public static final String STATIC_IDX = "def-static";
  public static final String DYNAMIC_IDX = "def-dynamic";
  public static final String MATCH_QUERY = "{\"query\":{\"match\":{\"%s\":\"%s\"}}}";
  // default number of records per dataset
  public static final int DEFAULT_REC_DATASET = 10;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(Map.class);

  public static EsIndexingPipelineOptions createPipelineOptions(
      EsServer server, String datasetKey, String idxName, String alias, int attempt) {
    String propertiesPath =
        Thread.currentThread().getContextClassLoader().getResource("lock.yaml").getPath();
    String[] args = {
      "--esIndexName=" + idxName,
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt,
      "--esAlias=" + alias,
      "--indexRefreshInterval=1ms",
      "--esHosts=" + server.getServerAddress(),
      "--esSchemaPath=dataset-mapping.json",
      "--properties=" + propertiesPath
    };
    return PipelineOptionsFactory.fromArgs(args).as(EsIndexingPipelineOptions.class);
  }

  public static Runnable indexingPipeline(
      EsServer server, EsIndexingPipelineOptions options, long numRecordsToIndex, String msg) {
    return () -> {
      String type = "doc";
      String document =
          "{\"datasetKey\" : \""
              + options.getDatasetId()
              + "\", \"crawlId\" : "
              + options.getAttempt()
              + ", \"msg\": \"%s\"}";

      LongStream.range(0, numRecordsToIndex)
          .forEach(
              i ->
                  EsService.indexDocument(
                      server.getEsClient(),
                      options.getEsIndexName(),
                      type,
                      i + options.getDatasetId().hashCode(),
                      String.format(document, msg + " " + i)));
      EsService.refreshIndex(server.getEsClient(), options.getEsIndexName());
    };
  }

  public static long countDocumentsFromQuery(EsServer server, String idxName, String query) {
    Response response = EsService.executeQuery(server.getEsClient(), idxName, query);
    try {
      return READER.readTree(response.getEntity().getContent()).get("hits").get("total").asLong();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void indexDatasets(
      EsServer server,
      List<String> datasets,
      int attempt,
      String indexName,
      String alias,
      long recordsPerDataset) {
    datasets.forEach(
        dataset -> {
          EsIndexingPipelineOptions options =
              createPipelineOptions(
                  server,
                  dataset,
                  Strings.isNullOrEmpty(indexName) ? dataset + "_" + attempt : indexName,
                  alias,
                  attempt);
          InterpretedToEsIndexExtendedPipeline.run(
              options,
              indexingPipeline(server, options, recordsPerDataset, options.getEsIndexName()));
        });
  }

  public static void indexDatasets(
      EsServer server, List<String> datasets, int attempt, String indexName, String alias) {
    indexDatasets(server, datasets, attempt, indexName, alias, DEFAULT_REC_DATASET);
  }
}
