package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.EsIntegrationTest;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.elasticsearch.client.Response;
import org.junit.After;
import org.junit.Test;

import static org.gbif.pipelines.esindexing.api.EsHandler.INDEX_SEPARATOR;
import static org.gbif.pipelines.esindexing.common.EsConstants.MAPPINGS_FIELD;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link EsHandler}.
 */
public class EsHandlerIT extends EsIntegrationTest {

  private static final String DATASET_TEST = "abc";
  private static final String ALIAS_TEST = "alias";
  private static final int DEFAULT_ATTEMPT = 1;

  @After
  public void cleanIndexes() {
    deleteAllIndexes();
  }

  @Test
  public void createIndexTest() {
    // create index
    String idxCreated = EsHandler.createIndex(getEsConfig(), DATASET_TEST, DEFAULT_ATTEMPT);

    // assert index created
    assertIndexWithSettingsAndIndexName(idxCreated, DATASET_TEST, DEFAULT_ATTEMPT);
  }

  @Test
  public void createIndexWithMappingsTest() {
    // create index
    String idxCreated = EsHandler.createIndex(getEsConfig(), DATASET_TEST, DEFAULT_ATTEMPT, Paths.get(TEST_MAPPINGS_PATH));

    // assert index created
    assertIndexWithSettingsAndIndexName(idxCreated, DATASET_TEST, DEFAULT_ATTEMPT);

    // assert mappings
    JsonNode mappings = getMappingsFromIndex(idxCreated).path(idxCreated).path(MAPPINGS_FIELD);
    assertTrue(mappings.has("doc"));
  }

  @Test
  public void swpaIndexInEmptyAliasTest() {
    // create index
    String idxCreated = EsHandler.createIndex(getEsConfig(), DATASET_TEST, 1);

    // swap index
    EsHandler.swapIndexInAlias(getEsConfig(), ALIAS_TEST, idxCreated);

    // assert result
    assertSwapResults(idxCreated, DATASET_TEST + INDEX_SEPARATOR + "*", ALIAS_TEST, Collections.emptySet());

    // check settings of index after swapping
    Response response = assertCreatedIndex(idxCreated);
    assertSearchSettings(response, idxCreated);
  }

  @Test
  public void swpaIndexInAliasTest() {
    // create index
    String idx1 = EsHandler.createIndex(getEsConfig(), DATASET_TEST, 1);
    String idx2 = EsHandler.createIndex(getEsConfig(), DATASET_TEST, 2);
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2));

    // add the indexes to the alias
    addIndexToAlias(ALIAS_TEST, initialIndexes);

    // create another index and swap it in the alias
    String idx3 = EsHandler.createIndex(getEsConfig(), DATASET_TEST, 3);
    EsHandler.swapIndexInAlias(getEsConfig(), ALIAS_TEST, idx3);

    // alias should have only the last index created
    assertSwapResults(idx3, DATASET_TEST + INDEX_SEPARATOR + "*", ALIAS_TEST, initialIndexes);

    // check settings of index after swapping
    Response response = assertCreatedIndex(idx3);
    assertSearchSettings(response, idx3);

    // create another index and swap it again
    String idx4 = EsHandler.createIndex(getEsConfig(), DATASET_TEST, 4);
    EsHandler.swapIndexInAlias(getEsConfig(), ALIAS_TEST, idx4);

    // alias should have only the last index created
    assertSwapResults(idx4, DATASET_TEST + INDEX_SEPARATOR + "*", ALIAS_TEST, Collections.singleton(idx3));

    // check settings of index after swapping
    response = assertCreatedIndex(idx4);
    assertSearchSettings(response, idx4);
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapWrongFormatIndexTest() {
    EsHandler.swapIndexInAlias(getEsConfig(), ALIAS_TEST, "dummy");
  }

  @Test(expected = IllegalStateException.class)
  public void swapMissingIndexTest() {
    EsHandler.swapIndexInAlias(getEsConfig(), ALIAS_TEST, "dummy_1");
  }

  /**
   * Utility mehtod to assert a newly created index.
   */
  private static void assertIndexWithSettingsAndIndexName(String idxCreated, String datasetId, int attempt) {
    // assert index created
    Response response = assertCreatedIndex(idxCreated);
    // assert index settings
    assertIndexingSettings(response, idxCreated);
    // assert idx name
    assertEquals(datasetId + INDEX_SEPARATOR + attempt, idxCreated);
  }

}
