package org.gbif.pipelines.ingest.pipelines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.pipelines.utils.EsServer;
import org.gbif.pipelines.ingest.pipelines.utils.ZkServer;

import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.ALIAS;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_2;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_3;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_4;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_5;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_6;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_7;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_8;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DATASET_TEST_9;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DEFAULT_REC_DATASET;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.DYNAMIC_IDX;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.MATCH_QUERY;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.STATIC_IDX;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.countDocumentsFromQuery;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.createPipelineOptions;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.indexDatasets;
import static org.gbif.pipelines.ingest.pipelines.utils.EsTestUtils.indexingPipeline;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Integration tests using an embedded ES instance to test the {@link InterpretedToEsIndexExtendedPipeline}.
 */
public class InterpretedToEsIndexExtendedPipelineIT {

  @ClassRule
  public static final EsServer ES_SERVER = new EsServer();

  @ClassRule
  public static final ZkServer ZK_SERVER = new ZkServer();

  /**
   * Tests the following cases:
   * <p>
   * 1. Index dataset in default static index.
   * 2. Reindex in the same index and with the same attempt the same records with changes in some fields.
   * 3. Reindex in the same index with new attempt and adding new records.
   * 4. Reindex in the same index with new attempt and deleting some records.
   */
  @Test
  public void reindexingDatasetInSameIndexTest() throws IOException {

    // State
    EsIndexingPipelineOptions options =
        createPipelineOptions(ES_SERVER, DATASET_TEST, STATIC_IDX, ALIAS, 1);
    // 1. Index the dataset for the first time
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));

    // When
    // 2. Reindex the same dataset with changes in one field and same attempt
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "second"));

    // Should
    assertEquals(DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
    assertEquals(DEFAULT_REC_DATASET,
        countDocumentsFromQuery(ES_SERVER, options.getEsIndexName(),
            String.format(MATCH_QUERY, "datasetKey", DATASET_TEST)));
    assertEquals(0,
        countDocumentsFromQuery(ES_SERVER, options.getEsIndexName(), String.format(MATCH_QUERY, "msg", "first")));
    assertEquals(DEFAULT_REC_DATASET,
        countDocumentsFromQuery(ES_SERVER, options.getEsIndexName(), String.format(MATCH_QUERY, "msg", "second")));
    assertEquals(DEFAULT_REC_DATASET,
        countDocumentsFromQuery(ES_SERVER, options.getEsIndexName(),
            String.format(MATCH_QUERY, "crawlId", options.getAttempt())));

    // When
    // 3. Reindex the same dataset with new attempt and more records
    options.setAttempt(2);
    InterpretedToEsIndexExtendedPipeline.run(options, indexingPipeline(ES_SERVER, options, 15, "second"));

    // Should
    assertEquals(15, EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
    assertEquals(15,
        countDocumentsFromQuery(ES_SERVER, options.getEsIndexName(),
            String.format(MATCH_QUERY, "crawlId", options.getAttempt())));

    // When
    // 4. Reindex the same dataset with new attempt and less records
    options.setAttempt(3);
    InterpretedToEsIndexExtendedPipeline.run(options, indexingPipeline(ES_SERVER, options, 7, "second"));

    // Should
    assertEquals(7, EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
    assertEquals(7,
        countDocumentsFromQuery(ES_SERVER, options.getEsIndexName(),
            String.format(MATCH_QUERY, "crawlId", options.getAttempt())));
  }

  /**
   * Tests the following cases:
   * <p>
   * 1. Index dataset in independent index.
   * 2. Reindex dataset in independent index with new attempt.
   * 3. Reindex dataset again in independent index with new attempt.
   */
  @Test
  public void swappingIndependentIndexesTest() {
    // State
    EsIndexingPipelineOptions options =
        createPipelineOptions(ES_SERVER, DATASET_TEST, DATASET_TEST + "_" + 1, ALIAS, 1);
    // 1. Index the dataset for the first time in independent index
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));

    // When
    // 2. Reindex in independent index with new attempt
    options.setAttempt(2);
    options.setEsIndexName(DATASET_TEST + "_" + 2);
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));

    // Should
    assertEquals(DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));

    // When
    // 3. Reindex again in independent index with new attempt
    options.setAttempt(3);
    options.setEsIndexName(DATASET_TEST + "_" + 3);
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));

    // Should
    assertEquals(DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
  }

  /**
   * Tests the following cases:
   * <p>
   * 1. Index dataset in default static index.
   * 2. Reindex dataset in default dynamic index.
   * 3. Reindex dataset back to the default static index.
   * 4. Reindex dataset to an independent index.
   * 5. Reindex dataset back to the default static index.
   */
  @Test
  public void indexSwitchingIndexTest() {
    // State
    EsIndexingPipelineOptions options =
        createPipelineOptions(ES_SERVER, DATASET_TEST, STATIC_IDX, ALIAS, 1);
    // 1. Index the dataset for the first time
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));
    // we add the index to alias -- currently we only add independent indexes to the alias
    EsService.swapIndexes(ES_SERVER.getEsClient(), Collections.singleton(ALIAS),
        Collections.singleton(options.getEsIndexName()),
        Collections.emptySet());

    // When
    // 2. Reindex dataset to a different index
    options.setEsIndexName(DYNAMIC_IDX);
    options.setAttempt(2);
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));
    // we add the index to alias -- currently we only add independent indexes to the alias
    EsService.swapIndexes(ES_SERVER.getEsClient(), Collections.singleton(ALIAS),
        Collections.singleton(options.getEsIndexName()),
        Collections.emptySet());

    // Should
    assertEquals(DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(0, EsIndex.countDocuments(ES_SERVER.getEsConfig(), STATIC_IDX));
    assertEquals(DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), DYNAMIC_IDX));

    // When
    // 3. Reindex dataset to default static
    options.setEsIndexName(STATIC_IDX);
    options.setAttempt(3);
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));

    // Should
    assertEquals(DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), STATIC_IDX));
    assertEquals(0, EsIndex.countDocuments(ES_SERVER.getEsConfig(), DYNAMIC_IDX));

    // When
    // 4. Switch dataset to independent index
    final String independentIndex = DATASET_TEST + "_" + 4;
    options.setEsIndexName(independentIndex);
    options.setAttempt(4);
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));

    // Should
    assertEquals(DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(0, EsIndex.countDocuments(ES_SERVER.getEsConfig(), STATIC_IDX));
    assertEquals(DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), independentIndex));

    // When
    // 5. Switch dataset back to default static index
    options.setEsIndexName(STATIC_IDX);
    options.setAttempt(5);
    InterpretedToEsIndexExtendedPipeline.run(options,
        indexingPipeline(ES_SERVER, options, DEFAULT_REC_DATASET, "first"));

    // Should
    assertEquals(DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), STATIC_IDX));
    assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), independentIndex));
  }

  @Test
  public void reindexingMultipleDatasetsTest() {
    // When
    // 1. Index multiple datasets

    int attempt = 1;
    // index in default static
    List<String> staticDatasets = ImmutableList.of(DATASET_TEST, DATASET_TEST_2, DATASET_TEST_3);
    indexDatasets(ES_SERVER, staticDatasets, attempt,
        STATIC_IDX,
        ALIAS, true);
    // index in default dynamic
    List<String> dynamicDatasets = ImmutableList.of(DATASET_TEST_4, DATASET_TEST_5, DATASET_TEST_6);
    indexDatasets(ES_SERVER, dynamicDatasets, attempt,
        DYNAMIC_IDX,
        ALIAS, true);
    // index some independent datasets
    List<String> independentDatasets = ImmutableList.of(DATASET_TEST_7, DATASET_TEST_8, DATASET_TEST_9);
    indexDatasets(ES_SERVER, independentDatasets, attempt, null,
        ALIAS, false);

    // Should
    assertEquals(DEFAULT_REC_DATASET * 9,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(DEFAULT_REC_DATASET * 3,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), STATIC_IDX));
    assertEquals(DEFAULT_REC_DATASET * 3,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), DYNAMIC_IDX));

    List<String> allDatasets = new ArrayList(staticDatasets);
    allDatasets.addAll(dynamicDatasets);
    allDatasets.addAll(independentDatasets);

    allDatasets.forEach(d ->
        assertEquals(DEFAULT_REC_DATASET,
            countDocumentsFromQuery(ES_SERVER, ALIAS, String.format(MATCH_QUERY, "datasetKey", d))));

    // When
    // 2. Switch some datasets to other indexes
    attempt = 2;
    indexDatasets(ES_SERVER, ImmutableList.of(DATASET_TEST, DATASET_TEST_9), attempt, DYNAMIC_IDX,
        ALIAS, false);
    indexDatasets(ES_SERVER, ImmutableList.of(DATASET_TEST_4, DATASET_TEST_7), attempt, STATIC_IDX,
        ALIAS, false);
    indexDatasets(ES_SERVER, ImmutableList.of(DATASET_TEST_2, DATASET_TEST_5), attempt, null,
        ALIAS, false);

    // Should
    assertEquals(DEFAULT_REC_DATASET * 9,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(DEFAULT_REC_DATASET * 3,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), STATIC_IDX));
    assertEquals(DEFAULT_REC_DATASET * 3,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), DYNAMIC_IDX));
    assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), DATASET_TEST_7 + "_1"));
    assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), DATASET_TEST_9 + "_1"));
    allDatasets.forEach(d ->
        assertEquals(DEFAULT_REC_DATASET,
            countDocumentsFromQuery(ES_SERVER, ALIAS, String.format(MATCH_QUERY, "datasetKey", d))));

    // When
    // 3. Switch some datasets again but adding and deleting records.
    attempt = 3;
    switchDatasetAndRecords(DATASET_TEST, DYNAMIC_IDX, STATIC_IDX, attempt, 5);

    // TODO: add more
  }

  private void switchDatasetAndRecords(String datasetKey, String sourceIdx, String targetIdx, int attempt,
      int diffRecords) {
    long previousCountAlias = EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS);
    long previousSourceCount = EsIndex.countDocuments(ES_SERVER.getEsConfig(), sourceIdx);
    long previousTargetCount = EsIndex.countDocuments(ES_SERVER.getEsConfig(), targetIdx);
    long previousRecordsDataset =
        countDocumentsFromQuery(ES_SERVER, ALIAS, String.format(MATCH_QUERY, "datasetKey", datasetKey));

    final long recordsDataset = previousRecordsDataset + diffRecords;

    indexDatasets(ES_SERVER, ImmutableList.of(datasetKey), attempt, targetIdx,
        ALIAS, false, recordsDataset);

    assertEquals(previousCountAlias + diffRecords,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS));
    assertEquals(previousTargetCount + recordsDataset,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), targetIdx));
    assertEquals(previousSourceCount - previousRecordsDataset,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), sourceIdx));
    assertEquals(recordsDataset,
        countDocumentsFromQuery(ES_SERVER, ALIAS, String.format(MATCH_QUERY, "datasetKey", datasetKey)));
  }


}
