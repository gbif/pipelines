package org.gbif.pipelines.estools.service;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.gbif.pipelines.estools.service.EsConstants.Field;
import static org.gbif.pipelines.estools.service.EsConstants.Searching;
import static org.gbif.pipelines.estools.service.EsConstants.Util.INDEX_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.elasticsearch.client.ResponseException;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link EsIndex}. */
public class EsIndexIT extends EsApiIntegration {

  private static final String DATASET_TEST = "abc";
  private static final String ALIAS_TEST = "alias";
  private static final int DEFAULT_ATTEMPT = 1;
  private static final String DEFAULT_IDX_NAME = DATASET_TEST + INDEX_SEPARATOR + DEFAULT_ATTEMPT;
  private static final String SEARCH = DATASET_TEST + INDEX_SEPARATOR + "*";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @After
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @Test
  public void createIndexTest() {
    // create index
    String idxCreated =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder().indexName(DEFAULT_IDX_NAME).settingsType(INDEXING).build());

    // assert index created
    assertIndexWithSettingsAndIndexName(idxCreated, DATASET_TEST, DEFAULT_ATTEMPT);
  }

  @Test
  public void createIndexWithMappingsTest() {
    // create index
    String idxCreated =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .indexName(DEFAULT_IDX_NAME)
                .settingsType(INDEXING)
                .pathMappings(TEST_MAPPINGS_PATH)
                .build());

    // assert index created
    assertIndexWithSettingsAndIndexName(idxCreated, DATASET_TEST, DEFAULT_ATTEMPT);

    // assert mappings
    JsonNode mappings = getMappingsFromIndex(idxCreated).path(idxCreated).path(Field.MAPPINGS);
    assertTrue(mappings.has("doc"));
  }

  @Test
  public void swapIndexInEmptyAliasTest() {
    // create index
    String idxCreated =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .datasetKey(DATASET_TEST)
                .attempt(1)
                .settingsType(INDEXING)
                .build());

    // swap index
    EsIndex.swapIndexInAliases(
        ES_SERVER.getEsConfig(), Collections.singleton(ALIAS_TEST), idxCreated);

    // assert result
    assertSwapResults(idxCreated, SEARCH, ALIAS_TEST, Collections.emptySet());

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxCreated));
    assertSearchSettings(idxCreated);
  }

  @Test
  public void swapIndexInAliasTest() {
    // create index
    String idx1 =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .datasetKey(DATASET_TEST)
                .attempt(1)
                .settingsType(INDEXING)
                .build());
    String idx2 =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .datasetKey(DATASET_TEST)
                .attempt(2)
                .settingsType(INDEXING)
                .build());
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2));

    // add the indexes to the alias
    addIndexesToAlias(ALIAS_TEST, initialIndexes);

    // create another index and swap it in the alias
    String idx3 =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .datasetKey(DATASET_TEST)
                .attempt(3)
                .settingsType(INDEXING)
                .build());
    EsIndex.swapIndexInAliases(ES_SERVER.getEsConfig(), Collections.singleton(ALIAS_TEST), idx3);

    // alias should have only the last index created
    assertSwapResults(idx3, SEARCH, ALIAS_TEST, initialIndexes);

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx3));
    assertSearchSettings(idx3);

    // create another index and swap it again
    String idx4 =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .datasetKey(DATASET_TEST)
                .attempt(4)
                .settingsType(INDEXING)
                .build());
    EsIndex.swapIndexInAliases(ES_SERVER.getEsConfig(), Collections.singleton(ALIAS_TEST), idx4);

    // alias should have only the last index created
    assertSwapResults(idx4, SEARCH, ALIAS_TEST, Collections.singleton(idx3));

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx4));
    assertSearchSettings(idx4);
  }

  @Test
  public void swapMissingIndexTest() {
    thrown.expect(ResponseException.class);
    thrown.expectMessage(
        CoreMatchers.containsString("URI [/_aliases], status line [HTTP/1.1 404 Not Found"));

    EsIndex.swapIndexInAliases(
        ES_SERVER.getEsConfig(), Collections.singleton(ALIAS_TEST), "dummy_1");
  }

  @Test
  public void countIndexDocumentsAfterSwappingTest() throws InterruptedException {
    // create index
    String idx =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .indexName(DEFAULT_IDX_NAME)
                .pathMappings(TEST_MAPPINGS_PATH)
                .build());

    // index some documents
    long n = 3;
    final String type = "doc";
    String document = "{\"test\" : \"test value\"}";
    for (int i = 1; i <= n; i++) {
      EsService.indexDocument(ES_SERVER.getEsClient(), idx, type, i, document);
    }

    // swap index in alias
    EsIndex.swapIndexInAliases(ES_SERVER.getEsConfig(), Collections.singleton(ALIAS_TEST), idx);

    // wait the refresh interval for the documents to become searchable.
    Thread.sleep(
        Long.parseLong(Iterables.get(Splitter.on('s').split(Searching.REFRESH_INTERVAL), 0)) * 1000
            + 500);

    // assert results against the alias
    assertEquals(n, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS_TEST));
    // assert results against the index
    assertEquals(n, EsIndex.countDocuments(ES_SERVER.getEsConfig(), idx));
  }

  /** Utility method to assert a newly created index. */
  private static void assertIndexWithSettingsAndIndexName(
      String idxCreated, String datasetId, int attempt) {
    // assert index created
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxCreated));
    // assert index settings
    assertIndexingSettings(idxCreated);
    // assert idx name
    assertEquals(datasetId + INDEX_SEPARATOR + attempt, idxCreated);
  }

  @Test
  public void findDatasetIndexesInAliasTest() {
    // create index
    final String datasetKey = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
    String idx1 =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder().datasetKey(DATASET_TEST).attempt(1).build());

    // index some documents
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";
    EsService.indexDocument(ES_SERVER.getEsClient(), idx1, "doc", 1, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), idx1);

    // add index to alias
    final String alias = "alias1";
    EsIndex.swapIndexInAliases(ES_SERVER.getEsConfig(), Collections.singleton(alias), idx1);

    // When
    Set<String> indexesFound =
        EsIndex.findDatasetIndexesInAliases(
            ES_SERVER.getEsConfig(), new String[] {alias, "fakeAlias"}, datasetKey);

    // Should
    assertEquals(1, indexesFound.size());
    assertTrue(indexesFound.contains(idx1));
  }

  @Test
  public void findDatasetIndexesInAliasesTest() {
    // create index
    final String datasetKey = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
    String idx1 =
        EsIndex.createIndex(
            ES_SERVER.getEsConfig(),
            IndexParams.builder().datasetKey(DATASET_TEST).attempt(1).build());

    // index some documents
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";
    EsService.indexDocument(ES_SERVER.getEsClient(), idx1, "doc", 1, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), idx1);

    // add index to aliases
    final String alias1 = "alias1";
    EsIndex.swapIndexInAliases(ES_SERVER.getEsConfig(), Collections.singleton(alias1), idx1);

    final String alias2 = "alias2";
    EsIndex.swapIndexInAliases(ES_SERVER.getEsConfig(), Collections.singleton(alias2), idx1);

    // When
    Set<String> indexesFound =
        EsIndex.findDatasetIndexesInAliases(
            ES_SERVER.getEsConfig(), new String[] {alias1, alias2}, datasetKey);

    // Should
    assertEquals(1, indexesFound.size());
    assertTrue(indexesFound.contains(idx1));
  }
}
