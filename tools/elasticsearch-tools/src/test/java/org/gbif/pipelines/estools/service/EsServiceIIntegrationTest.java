package org.gbif.pipelines.estools.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.gbif.pipelines.estools.service.EsConstants.Field;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.databind.JsonNode;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.gbif.pipelines.estools.common.SettingsType.SEARCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the {@link EsService}. */
public class EsServiceIIntegrationTest extends EsApiIntegration {

  private static final String ALIAS_TEST = "alias";

  /** {@link Rule} requires this field to be public. */
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @Test
  public void createIndexTest() throws IOException {

    // When
    String idx = EsService.createIndex(ES_SERVER.getEsClient(), "idx", INDEXING);

    Response response = ES_SERVER.getRestClient().performRequest(new Request(HttpGet.METHOD_NAME, "/" + idx));

    // Should
    assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
  }

  @Test
  public void createIndexWithSettingsAndMappingsTest() {

    // When
    String idx = EsService.createIndex(ES_SERVER.getEsClient(), "idx-settings", INDEXING, TEST_MAPPINGS_PATH);

    JsonNode mappings = getMappingsFromIndex(idx).path(idx).path(Field.MAPPINGS);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx));
    assertIndexingSettings(idx);
    assertTrue(mappings.has("doc"));
    assertTrue(mappings.path("doc").path("properties").has("test"));
    assertEquals("text", mappings.path("doc").path("properties").path("test").get("type").asText());
  }

  @Test
  public void createAndUpdateIndexWithSettingsTest() {

    // When
    String idx = EsService.createIndex(ES_SERVER.getEsClient(), "idx-settings", INDEXING);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx));
    assertIndexingSettings(idx);

    // When
    EsService.updateIndexSettings(ES_SERVER.getEsClient(), idx, SEARCH);

    // Should
    assertSearchSettings(idx);
  }

  @Test(expected = ResponseException.class)
  public void updateMissingIndexTest() {

    // When
    EsService.updateIndexSettings(ES_SERVER.getEsClient(), "fake-index", INDEXING);

    // Should
    thrown.expectMessage(CoreMatchers.containsString("Error updating index"));
  }

  @Test(expected = ResponseException.class)
  public void createWrongIndexTest() {

    // When
    EsService.createIndex(ES_SERVER.getEsClient(), "UPPERCASE", INDEXING);

    // Should
    thrown.expectMessage(CoreMatchers.containsString("must be lowercase"));
  }

  @Test(expected = ResponseException.class)
  public void duplicatedIndexTest() {

    // State
    EsService.createIndex(ES_SERVER.getEsClient(), "idx", INDEXING);

    // When
    EsService.createIndex(ES_SERVER.getEsClient(), "idx", INDEXING);

    // Should
    thrown.expectMessage(CoreMatchers.containsString("already exists"));
  }

  @Test
  public void getIndexesByAliasAndSwapIndexTest() {

    // State
    String idx1 = EsService.createIndex(ES_SERVER.getEsClient(), "idx1", INDEXING);
    String idx2 = EsService.createIndex(ES_SERVER.getEsClient(), "idx2", INDEXING);
    String idx3 = EsService.createIndex(ES_SERVER.getEsClient(), "idx3", INDEXING);
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2, idx3));

    addIndexesToAlias(ALIAS_TEST, initialIndexes);

    String idx4 = EsService.createIndex(ES_SERVER.getEsClient(), "idx4", INDEXING);
    String idx5 = EsService.createIndex(ES_SERVER.getEsClient(), "idx5", INDEXING);

    // When
    Set<String> indexes =
        EsService.getIndexesByAliasAndIndexPattern(ES_SERVER.getEsClient(), "idx*", ALIAS_TEST);

    // Should
    assertEquals(3, indexes.size());
    assertTrue(indexes.containsAll(initialIndexes));

    // When
    EsService.swapIndexes(ES_SERVER.getEsClient(), ALIAS_TEST, Collections.singleton(idx4), initialIndexes);

    // Should
    assertSwapResults(idx4, "idx*", ALIAS_TEST, initialIndexes);

    // When
    EsService.swapIndexes(ES_SERVER.getEsClient(), ALIAS_TEST, Collections.singleton(idx5),
        Collections.singleton(idx4));

    // Should
    assertSwapResults(idx5, "idx*", ALIAS_TEST, Collections.singleton(idx4));
  }

  @Test
  public void getIndexesFromMissingAliasTest() {

    // When
    Set<String> idx = EsService.getIndexesByAliasAndIndexPattern(ES_SERVER.getEsClient(), "idx*", "fake-alias");

    // Should
    assertTrue(idx.isEmpty());
  }

  @Test
  public void swapEmptyAliasTest() {

    // State
    String idx1 = EsService.createIndex(ES_SERVER.getEsClient(), "idx1", INDEXING);

    // When
    EsService.swapIndexes(ES_SERVER.getEsClient(), ALIAS_TEST, Collections.singleton(idx1), Collections.emptySet());

    // Should
    assertSwapResults(idx1, "idx*", ALIAS_TEST, Collections.emptySet());
  }

  @Test(expected = ResponseException.class)
  public void swapMissingIndexTest() {

    // When
    EsService.swapIndexes(ES_SERVER.getEsClient(), "fake-alias", Collections.singleton("fake-index"),
        Collections.emptySet());

    // Should
    thrown.expectMessage(CoreMatchers.containsString("afwfawf"));
  }

  @Test
  public void countEmptyIndexTest() {

    // When
    String idx = EsService.createIndex(ES_SERVER.getEsClient(), "idx_1", SEARCH);

    // Should
    assertEquals(0L, EsService.countIndexDocuments(ES_SERVER.getEsClient(), idx));
  }

  /**
   * It also tests indirectly the methods {@link EsService#indexDocument}, {@link
   * EsService#deleteDocument} and {@link EsService#refreshIndex}
   */
  @Test
  public void countIndexDocumentsTest() {

    // State
    String idx =
        EsService.createIndex(ES_SERVER.getEsClient(), "idx_1", Collections.emptyMap(), TEST_MAPPINGS_PATH);

    // When
    // index some documents
    long n = 3;
    final String type = "doc";
    String document = "{\"test\" : \"test value\"}";
    for (int i = 1; i <= n; i++) {
      EsService.indexDocument(ES_SERVER.getEsClient(), idx, type, i, document);
    }

    // Should
    // they shouldn't be searchable yet.
    assertEquals(0, EsService.countIndexDocuments(ES_SERVER.getEsClient(), idx));

    // When
    // refresh the index to make all the documents searchable.
    EsService.refreshIndex(ES_SERVER.getEsClient(), idx);

    // Should
    assertEquals(n, EsService.countIndexDocuments(ES_SERVER.getEsClient(), idx));

    // When
    // delete last document
    EsService.deleteDocument(ES_SERVER.getEsClient(), idx, type, n);
    EsService.refreshIndex(ES_SERVER.getEsClient(), idx);

    // Should
    assertEquals(n - 1, EsService.countIndexDocuments(ES_SERVER.getEsClient(), idx));
  }

  @Test(expected = ResponseException.class)
  public void countMissingIndexTest() {

    // When
    EsService.countIndexDocuments(ES_SERVER.getEsClient(), "fake");

    // Should
    thrown.expectMessage(CoreMatchers.containsString("no such index"));
  }

  @Test
  public void existsIndexTest() {

    // State
    String idx1 = EsService.createIndex(ES_SERVER.getEsClient(), "idx1", INDEXING);

    // When
    boolean exists = EsService.existsIndex(ES_SERVER.getEsClient(), idx1);

    // Should
    assertTrue(exists);
  }

  @Test
  public void existsMissingIndexTest() {

    // When
    boolean exists = EsService.existsIndex(ES_SERVER.getEsClient(), "missing");

    // Should
    assertFalse(exists);
  }

  @Test
  public void deleteAllIndicesTest() {

    // When
    String idx1 = EsService.createIndex(ES_SERVER.getEsClient(), "idx1", INDEXING);
    String idx2 = EsService.createIndex(ES_SERVER.getEsClient(), "idx2", INDEXING);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx1));
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx2));

    // When
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());

    // Should
    assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), idx1));
    assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), idx2));
  }

  @Test
  public void findDatasetIndexesInAliasTest() {

    // State
    String idx1 = EsService.createIndex(ES_SERVER.getEsClient(), "idx1", INDEXING);
    String idx2 = EsService.createIndex(ES_SERVER.getEsClient(), "idx2", INDEXING);
    Set<String> indexes = new HashSet<>();
    indexes.add(idx1);
    indexes.add(idx2);

    // we create another empty index to check that it's discarded
    String idx3 = EsService.createIndex(ES_SERVER.getEsClient(), "idx3", INDEXING);

    // index some documents
    final String type = "doc";
    final String datasetKey = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";

    for (String index : indexes) {
      EsService.indexDocument(ES_SERVER.getEsClient(), index, type, 1, document);
      EsService.refreshIndex(ES_SERVER.getEsClient(), index);
    }

    final String alias = "alias1";
    EsService.swapIndexes(ES_SERVER.getEsClient(), alias, indexes, Collections.emptySet());

    // When
    Set<String> indexesFound = EsService.findDatasetIndexesInAlias(ES_SERVER.getEsClient(), alias, datasetKey);

    // Should
    assertEquals(2, indexesFound.size());
    assertTrue(indexesFound.contains(idx1));
    assertTrue(indexesFound.contains(idx2));

    // When
    indexesFound = EsService.findDatasetIndexesInAlias(ES_SERVER.getEsClient(), alias, "fakeDataset");

    // State
    assertTrue(indexesFound.isEmpty());
  }
}
