package org.gbif.pipelines.estools.service;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.gbif.pipelines.estools.common.SettingsType.SEARCH;
import static org.gbif.pipelines.estools.service.EsQueries.DELETE_BY_DATASET_QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.gbif.pipelines.estools.model.DeleteByQueryTask;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsConstants.Field;
import org.gbif.pipelines.estools.service.EsConstants.Indexing;
import org.gbif.pipelines.estools.service.EsConstants.Searching;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link EsService}. */
public class EsServiceIT extends EsApiIntegration {

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createIndexTest() throws IOException {

    // When
    String idx =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder().indexName("create-index-test").settingsType(INDEXING).build());

    Response response =
        EsApiIntegration.ES_SERVER
            .getRestClient()
            .performRequest(new Request(HttpGet.METHOD_NAME, "/" + idx));

    // Should
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
  }

  @Test
  public void createIndexWithSettingsAndMappingsTest() {

    // When
    String idx =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("create-index-with-settings-and-mappings-test")
                .settingsType(INDEXING)
                .pathMappings(EsApiIntegration.TEST_MAPPINGS_PATH)
                .build());

    JsonNode mappings = EsApiIntegration.getMappingsFromIndex(idx).path(idx).path(Field.MAPPINGS);

    // Should
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx));
    EsApiIntegration.assertIndexingSettings(idx);
    assertTrue(mappings.path("properties").has("test"));
    assertEquals("text", mappings.path("properties").path("test").get("type").asText());
  }

  @Test
  public void createAndUpdateIndexWithSettingsTest() {

    // When
    String idx =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("create-and-update-index-with-settings-test")
                .settingsType(INDEXING)
                .build());

    // Should
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx));
    EsApiIntegration.assertIndexingSettings(idx);

    // When
    EsService.updateIndexSettings(
        EsApiIntegration.ES_SERVER.getEsClient(), idx, Searching.getDefaultSearchSettings());

    // Should
    EsApiIntegration.assertSearchSettings(idx);
  }

  @Test(expected = ResponseException.class)
  public void updateMissingIndexTest() {

    // When
    EsService.updateIndexSettings(
        EsApiIntegration.ES_SERVER.getEsClient(),
        "update-missing-index-test",
        Indexing.getDefaultIndexingSettings());

    // Should
    thrown.expectMessage(CoreMatchers.containsString("Error updating index"));
  }

  @Test(expected = ResponseException.class)
  public void createWrongIndexTest() {

    // When
    EsService.createIndex(
        EsApiIntegration.ES_SERVER.getEsClient(),
        IndexParams.builder().indexName("UPPERCASE").settingsType(INDEXING).build());

    // Should
    thrown.expectMessage(CoreMatchers.containsString("must be lowercase"));
  }

  @Test(expected = ResponseException.class)
  public void duplicatedIndexTest() {

    // State
    EsService.createIndex(
        EsApiIntegration.ES_SERVER.getEsClient(),
        IndexParams.builder().indexName("duplicated-index-test").settingsType(INDEXING).build());

    // When
    EsService.createIndex(
        EsApiIntegration.ES_SERVER.getEsClient(),
        IndexParams.builder().indexName("duplicated-index-test").settingsType(INDEXING).build());

    // Should
    thrown.expectMessage(CoreMatchers.containsString("already exists"));
  }

  @Test
  public void getIndexesByAliasAndSwapIndexTest() {

    String alias = "get-indexes-by-alias-and-swap-index";

    // State
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("get-indexes-by-alias-and-swap-index-test-1")
                .settingsType(INDEXING)
                .build());
    String idx2 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("get-indexes-by-alias-and-swap-index-test-2")
                .settingsType(INDEXING)
                .build());
    String idx3 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("get-indexes-by-alias-and-swap-index-test-3")
                .settingsType(INDEXING)
                .build());
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2, idx3));

    EsApiIntegration.addIndexesToAlias(alias, initialIndexes);

    String idx4 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("get-indexes-by-alias-and-swap-index-test-4")
                .settingsType(INDEXING)
                .build());
    String idx5 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("get-indexes-by-alias-and-swap-index-test-5")
                .settingsType(INDEXING)
                .build());

    // When
    Set<String> indexes =
        EsService.getIndexesByAliasAndIndexPattern(
            EsApiIntegration.ES_SERVER.getEsClient(),
            "get-indexes-by-alias-and-swap-index-test-*",
            alias);

    // Should
    assertEquals(3, indexes.size());
    assertTrue(indexes.containsAll(initialIndexes));

    // When
    EsService.swapIndexes(
        EsApiIntegration.ES_SERVER.getEsClient(),
        Collections.singleton(alias),
        Collections.singleton(idx4),
        initialIndexes);

    // Should
    EsApiIntegration.assertSwapResults(
        idx4, "get-indexes-by-alias-and-swap-index-test-*", alias, initialIndexes);

    // When
    EsService.swapIndexes(
        EsApiIntegration.ES_SERVER.getEsClient(),
        Collections.singleton(alias),
        Collections.singleton(idx5),
        Collections.singleton(idx4));

    // Should
    EsApiIntegration.assertSwapResults(
        idx5, "get-indexes-by-alias-and-swap-index-test-*", alias, Collections.singleton(idx4));
  }

  @Test
  public void swapInMultipleAliasesTest() {

    String alias1 = "swap-in-multiple-aliases-1";
    String alias2 = "swap-in-multiple-aliases-2";

    // State
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("swap-in-multiple-aliases-test-1")
                .settingsType(INDEXING)
                .build());
    String idx2 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("swap-in-multiple-aliases-test-2")
                .settingsType(INDEXING)
                .build());
    String idx3 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("swap-in-multiple-aliases-test-3")
                .settingsType(INDEXING)
                .build());
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2, idx3));

    Set<String> aliases = new HashSet<>(Arrays.asList(alias1, alias2));
    EsApiIntegration.addIndexesToAliases(aliases, initialIndexes);

    String idx4 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("swap-in-multiple-aliases-test-4")
                .settingsType(INDEXING)
                .build());
    String idx5 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("swap-in-multiple-aliases-test-5")
                .settingsType(INDEXING)
                .build());

    // When
    Set<String> indexes =
        EsService.getIndexesByAliasAndIndexPattern(
            EsApiIntegration.ES_SERVER.getEsClient(), "swap-in-multiple-aliases-test-*", aliases);

    // Should
    assertEquals(3, indexes.size());
    assertTrue(indexes.containsAll(initialIndexes));

    // When
    EsService.swapIndexes(
        EsApiIntegration.ES_SERVER.getEsClient(),
        aliases,
        Collections.singleton(idx4),
        initialIndexes);

    // Should
    EsApiIntegration.assertSwapResults(
        idx4, "swap-in-multiple-aliases-test-*", aliases, initialIndexes);

    // When
    EsService.swapIndexes(
        EsApiIntegration.ES_SERVER.getEsClient(),
        aliases,
        Collections.singleton(idx5),
        Collections.singleton(idx4));

    // Should
    EsApiIntegration.assertSwapResults(
        idx5, "swap-in-multiple-aliases-test-*", aliases, Collections.singleton(idx4));
  }

  @Test
  public void getIndexesFromMissingAliasTest() {

    // When
    Set<String> idx =
        EsService.getIndexesByAliasAndIndexPattern(
            EsApiIntegration.ES_SERVER.getEsClient(),
            "get-indexes-from-missing-alias-test*",
            "fake-alias");

    // Should
    assertTrue(idx.isEmpty());
  }

  @Test
  public void swapEmptyAliasTest() {

    String alias = "swap-empty-alias";

    // State
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("swap-empty-alias-test")
                .settingsType(INDEXING)
                .build());

    // When
    EsService.swapIndexes(
        EsApiIntegration.ES_SERVER.getEsClient(),
        Collections.singleton(alias),
        Collections.singleton(idx1),
        Collections.emptySet());

    // Should
    EsApiIntegration.assertSwapResults(idx1, "swap-empty-alias-*", alias, Collections.emptySet());
  }

  @Test(expected = ResponseException.class)
  public void swapMissingIndexTest() {

    // When
    EsService.swapIndexes(
        EsApiIntegration.ES_SERVER.getEsClient(),
        Collections.singleton("swap-missing-index-test-alias"),
        Collections.singleton("swap-missing-index-test-index"),
        Collections.emptySet());

    // Should
    thrown.expectMessage(CoreMatchers.containsString("afwfawf"));
  }

  @Test
  public void countEmptyIndexTest() {

    // When
    String idx =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder().indexName("count-empty-index-test").settingsType(SEARCH).build());

    // Should
    assertEquals(0L, EsService.countIndexDocuments(EsApiIntegration.ES_SERVER.getEsClient(), idx));
  }

  /**
   * It also tests indirectly the methods {@link EsService#indexDocument}, {@link
   * EsService#deleteDocument} and {@link EsService#refreshIndex}
   */
  @Test
  public void countIndexDocumentsTest() {

    // State
    String idx =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("count-index-documents-test")
                .pathMappings(EsApiIntegration.TEST_MAPPINGS_PATH)
                .build());

    // When
    // index some documents
    long n = 3;
    String document = "{\"test\" : \"test value\"}";
    for (int i = 1; i <= n; i++) {
      EsService.indexDocument(EsApiIntegration.ES_SERVER.getEsClient(), idx, i, document);
    }

    // Should
    // they shouldn't be searchable yet.
    assertEquals(0, EsService.countIndexDocuments(EsApiIntegration.ES_SERVER.getEsClient(), idx));

    // When
    // refresh the index to make all the documents searchable.
    EsService.refreshIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx);

    // Should
    assertEquals(n, EsService.countIndexDocuments(EsApiIntegration.ES_SERVER.getEsClient(), idx));

    // When
    // delete last document
    EsService.deleteDocument(EsApiIntegration.ES_SERVER.getEsClient(), idx, n);
    EsService.refreshIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx);

    // Should
    assertEquals(
        n - 1, EsService.countIndexDocuments(EsApiIntegration.ES_SERVER.getEsClient(), idx));
  }

  @Test(expected = ResponseException.class)
  public void countMissingIndexTest() {

    // When
    EsService.countIndexDocuments(
        EsApiIntegration.ES_SERVER.getEsClient(), "count-missing-index-test");

    // Should
    thrown.expectMessage(CoreMatchers.containsString("no such index"));
  }

  @Test
  public void existsIndexTest() {

    // State
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder().indexName("exists-index-test").settingsType(INDEXING).build());

    // When
    boolean exists = EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx1);

    // Should
    assertTrue(exists);
  }

  @Test
  public void existsMissingIndexTest() {

    // When
    boolean exists =
        EsService.existsIndex(
            EsApiIntegration.ES_SERVER.getEsClient(), "exists-missing-index-test");

    // Should
    assertFalse(exists);
  }

  @Test
  public void deleteAllIndicesTest() {

    // When
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("delete-all-indices-test-1")
                .settingsType(INDEXING)
                .build());
    String idx2 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("delete-all-indices-test-2")
                .settingsType(INDEXING)
                .build());

    // Should
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx1));
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx2));

    // When
    EsService.deleteAllIndexes(EsApiIntegration.ES_SERVER.getEsClient());

    // Should
    assertFalse(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx1));
    assertFalse(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx2));
  }

  @Test
  public void findDatasetIndexesInAliasTest() {

    // State
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("find-dataset-indexes-in-alias-test-1")
                .settingsType(INDEXING)
                .build());
    String idx2 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("find-dataset-indexes-in-alias-test-2")
                .settingsType(INDEXING)
                .build());
    Set<String> indexes = new HashSet<>();
    indexes.add(idx1);
    indexes.add(idx2);

    // we create another empty index to check that it's discarded
    EsService.createIndex(
        EsApiIntegration.ES_SERVER.getEsClient(),
        IndexParams.builder()
            .indexName("find-dataset-indexes-in-alias-test-3")
            .settingsType(INDEXING)
            .build());

    // index some documents
    final String datasetKey = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";

    for (String index : indexes) {
      EsService.indexDocument(EsApiIntegration.ES_SERVER.getEsClient(), index, 1, document);
      EsService.refreshIndex(EsApiIntegration.ES_SERVER.getEsClient(), index);
    }

    final String alias = "alias-find-dataset-indexes-in-alias";
    EsService.swapIndexes(
        EsApiIntegration.ES_SERVER.getEsClient(),
        Collections.singleton(alias),
        indexes,
        Collections.emptySet());

    // When
    Set<String> indexesFound =
        EsService.findDatasetIndexesInAlias(
            EsApiIntegration.ES_SERVER.getEsClient(), alias, datasetKey);

    // Should
    assertEquals(2, indexesFound.size());
    assertTrue(indexesFound.contains(idx1));
    assertTrue(indexesFound.contains(idx2));

    // When
    indexesFound =
        EsService.findDatasetIndexesInAlias(
            EsApiIntegration.ES_SERVER.getEsClient(), alias, "fakeDataset");

    // State
    assertTrue(indexesFound.isEmpty());
  }

  @Test
  public void deleteByQueryTest() {

    // State
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder().indexName("delete-by-query-test").settingsType(SEARCH).build());

    // index some documents
    final String datasetKey = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";
    IntStream.range(1, 4)
        .forEach(
            i ->
                EsService.indexDocument(
                    EsApiIntegration.ES_SERVER.getEsClient(), idx1, i, document));

    // When
    String query = String.format(DELETE_BY_DATASET_QUERY, datasetKey);
    String taskId =
        EsService.deleteRecordsByQuery(EsApiIntegration.ES_SERVER.getEsClient(), idx1, query);

    // Should
    assertFalse(Strings.isNullOrEmpty(taskId));
  }

  @Test
  public void getDeleteByQueryTaskTest() throws InterruptedException {

    // State
    String idx1 =
        EsService.createIndex(
            EsApiIntegration.ES_SERVER.getEsClient(),
            IndexParams.builder()
                .indexName("get-delete-by-query-tas-test")
                .settingsType(SEARCH)
                .build());

    // index some documents
    final String datasetKey = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";
    IntStream.range(1, 6)
        .forEach(
            i ->
                EsService.indexDocument(
                    EsApiIntegration.ES_SERVER.getEsClient(), idx1, i, document));
    EsService.refreshIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx1);

    // When
    String query = String.format(DELETE_BY_DATASET_QUERY, datasetKey);
    String taskId =
        EsService.deleteRecordsByQuery(EsApiIntegration.ES_SERVER.getEsClient(), idx1, query);
    DeleteByQueryTask task =
        EsService.getDeletedByQueryTask(EsApiIntegration.ES_SERVER.getEsClient(), taskId);

    // Should
    assertNotNull(task);
    assertFalse(task.isCompleted());

    // When
    TimeUnit.SECONDS.sleep(1);
    task = EsService.getDeletedByQueryTask(EsApiIntegration.ES_SERVER.getEsClient(), taskId);
    assertTrue(task.isCompleted());
    assertEquals(5, task.getRecordsDeleted());
  }

  @Test
  public void buildEndpointTest() {
    assertEquals("/1/2/3", EsService.buildEndpoint("1", "2", "3"));
  }
}
