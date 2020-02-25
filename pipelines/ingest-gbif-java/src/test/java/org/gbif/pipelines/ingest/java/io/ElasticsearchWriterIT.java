package org.gbif.pipelines.ingest.java.io;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.io.avro.BasicRecord;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.INDEX_TYPE;
import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.gbif.pipelines.estools.service.EsService.buildEndpoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticsearchWriterIT {

  // files for testing
  static final Path TEST_MAPPINGS_PATH = Paths.get("mappings/simple-mapping.json");

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule
  public static final EsServer ES_SERVER = new EsServer();

  /** {@link Rule} requires this field to be public. */
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @Test
  public void oneRecordsSyncTest() {
    // State
    String idxName = "single-record-sync-test";
    List<BasicRecord> basicRecordList = generateBrList(0);
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(true)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }

  @Test
  public void oneRecordsAsyncTest() {
    // State
    String idxName = "single-record-async-test";
    List<BasicRecord> basicRecordList = generateBrList(0);
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(false)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }

  @Test
  public void oneHundredRecordsSyncTest() {
    // State
    String idxName = "one-hundred-record-sync-test";
    List<BasicRecord> basicRecordList = generateBrList(99);
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(true)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }

  @Test
  public void oneHundredRecordsAsyncTest() {
    // State
    String idxName = "one-hundred-record-async-test";
    List<BasicRecord> basicRecordList = generateBrList(99);
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(false)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }

  @Test
  public void thirtyRecordsSyncBigBatchTest() {
    // State
    String idxName = "thirty-record-sync-big-batchtest";
    List<BasicRecord> basicRecordList = generateBrList(29);
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250_000L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(true)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }

  @Test
  public void thirtyRecordsAsyncBigBatchTest() {
    // State
    String idxName = "thirty-record-async-big-batch-test";
    List<BasicRecord> basicRecordList = generateBrList(29);
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250_000L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(false)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }


  @Test
  public void zeroRecordsSyncBigBatchTest() {
    // State
    String idxName = "zero-record-sync-big-batchtest";
    List<BasicRecord> basicRecordList = Collections.emptyList();
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250_000L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(true)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }

  @Test
  public void zeroRecordsAsyncBigBatchTest() {
    // State
    String idxName = "zero-record-async-big-batch-test";
    List<BasicRecord> basicRecordList = Collections.emptyList();
    createIndex(idxName);

    // When
    ElasticsearchWriter.<BasicRecord>builder()
        .esHosts(ES_SERVER.getEsConfig().getRawHosts())
        .esMaxBatchSize(10L)
        .esMaxBatchSizeBytes(250_000L)
        .executor(Executors.newSingleThreadExecutor())
        .useSyncMode(false)
        .indexRequestFn(createindexRequestFn(idxName))
        .records(basicRecordList)
        .build()
        .write();

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(basicRecordList.size(), EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }

  private static List<BasicRecord> generateBrList(int count) {
    return IntStream.rangeClosed(0, count)
        .boxed()
        .map(x -> BasicRecord.newBuilder().setId(Integer.toString(x)).build())
        .collect(Collectors.toList());
  }

  private static Function<BasicRecord, IndexRequest> createindexRequestFn(String idxName) {
    return br -> {
      String k = br.getId();
      String dummyJson = "{\"test\": \"text\"}";
      return new IndexRequest(idxName, INDEX_TYPE, k).source(dummyJson, JSON);
    };
  }

  /** Utility method to create an index. */
  private static void createIndex(String idxName) {
    String idx = EsService.createIndex(ES_SERVER.getEsClient(), IndexParams.builder()
        .indexName(idxName)
        .settingsType(INDEXING)
        .pathMappings(TEST_MAPPINGS_PATH)
        .build());
    String endpoint = buildEndpoint(idx, "_mapping");
    try {
      RestClient client = ES_SERVER.getRestClient();
      client.performRequest(new Request(HttpGet.METHOD_NAME, endpoint));
    } catch (IOException e) {
      throw new AssertionError("Could not get the index mappings", e);
    }
  }

}
