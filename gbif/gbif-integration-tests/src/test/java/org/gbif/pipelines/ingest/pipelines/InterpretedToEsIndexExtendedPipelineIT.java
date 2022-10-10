package org.gbif.pipelines.ingest.pipelines;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.ingest.utils.EsServer;
import org.gbif.pipelines.ingest.utils.EsTestUtils;
import org.gbif.pipelines.ingest.utils.ZkServer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests using an embedded ES instance to test the {@link
 * InterpretedToEsIndexExtendedPipeline}.
 */
public class InterpretedToEsIndexExtendedPipelineIT {

  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();

  /**
   * Tests the following cases:
   *
   * <p>1. Index dataset in default static index. 2. Reindex in the same index and with the same
   * attempt the same records with changes in some fields. 3. Reindex in the same index with new
   * attempt and adding new records. 4. Reindex in the same index with new attempt and deleting some
   * records.
   */
  @Test
  public void reindexingDatasetInSameDefaultIndexTest() {

    // State
    String defIndex = "def-reindexing-dataset-in-same-default-index-test";
    String alias = "alias-reindexing-dataset-in-same-default-index-test";
    EsIndexingPipelineOptions options =
        EsTestUtils.createPipelineOptions(ES_SERVER, EsTestUtils.DATASET_TEST, defIndex, alias, 1);
    // 1. Index the dataset for the first time
    InterpretedToEsIndexExtendedPipeline.run(
        options,
        EsTestUtils.indexingPipeline(ES_SERVER, options, EsTestUtils.DEFAULT_REC_DATASET, "first"));

    // When
    // 2. Reindex the same dataset with changes in one field and same attempt
    InterpretedToEsIndexExtendedPipeline.run(
        options,
        EsTestUtils.indexingPipeline(
            ES_SERVER, options, EsTestUtils.DEFAULT_REC_DATASET, "second"));

    // Should
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET,
        EsTestUtils.countDocumentsFromQuery(
            ES_SERVER,
            options.getEsIndexName(),
            String.format(EsTestUtils.MATCH_QUERY, "datasetKey", EsTestUtils.DATASET_TEST)));
    Assert.assertEquals(
        0,
        EsTestUtils.countDocumentsFromQuery(
            ES_SERVER,
            options.getEsIndexName(),
            String.format(EsTestUtils.MATCH_QUERY, "msg", "first")));
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET,
        EsTestUtils.countDocumentsFromQuery(
            ES_SERVER,
            options.getEsIndexName(),
            String.format(EsTestUtils.MATCH_QUERY, "msg", "second")));
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET,
        EsTestUtils.countDocumentsFromQuery(
            ES_SERVER,
            options.getEsIndexName(),
            String.format(EsTestUtils.MATCH_QUERY, "crawlId", options.getAttempt())));

    // When
    // 3. Reindex the same dataset with new attempt and more records
    options.setAttempt(2);
    InterpretedToEsIndexExtendedPipeline.run(
        options, EsTestUtils.indexingPipeline(ES_SERVER, options, 15, "second"));

    // Should
    assertEquals(15, EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
    Assert.assertEquals(
        15,
        EsTestUtils.countDocumentsFromQuery(
            ES_SERVER,
            options.getEsIndexName(),
            String.format(EsTestUtils.MATCH_QUERY, "crawlId", options.getAttempt())));

    // When
    // 4. Reindex the same dataset with new attempt and less records
    options.setAttempt(3);
    InterpretedToEsIndexExtendedPipeline.run(
        options, EsTestUtils.indexingPipeline(ES_SERVER, options, 7, "second"));

    // Should
    assertEquals(7, EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
    Assert.assertEquals(
        7,
        EsTestUtils.countDocumentsFromQuery(
            ES_SERVER,
            options.getEsIndexName(),
            String.format(EsTestUtils.MATCH_QUERY, "crawlId", options.getAttempt())));
  }

  /**
   * Tests the following cases:
   *
   * <p>1. Index dataset in independent index. 2. Reindex dataset in independent index with new
   * attempt. 3. Reindex dataset again in independent index with new attempt.
   */
  @Test
  public void swappingIndependentIndexesTest() {
    // State
    String alias = "alias-swapping-independent-indexes-test";
    EsIndexingPipelineOptions options =
        EsTestUtils.createPipelineOptions(
            ES_SERVER, EsTestUtils.DATASET_TEST, EsTestUtils.DATASET_TEST + "_" + 1, alias, 1);
    // 1. Index the dataset for the first time in independent index
    InterpretedToEsIndexExtendedPipeline.run(
        options,
        EsTestUtils.indexingPipeline(ES_SERVER, options, EsTestUtils.DEFAULT_REC_DATASET, "first"));

    // When
    // 2. Reindex in independent index with new attempt
    options.setAttempt(2);
    options.setEsIndexName(EsTestUtils.DATASET_TEST + "_" + 2);
    InterpretedToEsIndexExtendedPipeline.run(
        options,
        EsTestUtils.indexingPipeline(ES_SERVER, options, EsTestUtils.DEFAULT_REC_DATASET, "first"));

    // Should
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), alias));
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));

    // When
    // 3. Reindex again in independent index with new attempt
    options.setAttempt(3);
    options.setEsIndexName(EsTestUtils.DATASET_TEST + "_" + 3);
    InterpretedToEsIndexExtendedPipeline.run(
        options,
        EsTestUtils.indexingPipeline(ES_SERVER, options, EsTestUtils.DEFAULT_REC_DATASET, "first"));

    // Should
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET, EsIndex.countDocuments(ES_SERVER.getEsConfig(), alias));
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), options.getEsIndexName()));
  }

  /**
   * Tests the following cases: 1. Index multiple datasets in default indexes and independent ones.
   * 2. Switch datasets to different indexes without changing the number of records. 3. Switch
   * datasets to different indexes and changing the number of records at the same time.
   */
  @Test
  public void switchingIndexTest() {
    // When
    // 1. Index multiple datasets
    int attempt = 1;
    String defIndex = "def-switching-index-test";
    String dynIndex = "dyn-switching-index-test";
    String alias = "alias-switching-index-test";
    // index in default static
    List<String> staticDatasets =
        Arrays.asList(
            EsTestUtils.DATASET_TEST, EsTestUtils.DATASET_TEST_2, EsTestUtils.DATASET_TEST_3);
    EsTestUtils.indexDatasets(ES_SERVER, staticDatasets, attempt, defIndex, alias);
    // index in default dynamic
    List<String> dynamicDatasets =
        Arrays.asList(
            EsTestUtils.DATASET_TEST_4, EsTestUtils.DATASET_TEST_5, EsTestUtils.DATASET_TEST_6);
    EsTestUtils.indexDatasets(ES_SERVER, dynamicDatasets, attempt, dynIndex, alias);
    // index some independent datasets
    List<String> independentDatasets =
        Arrays.asList(
            EsTestUtils.DATASET_TEST_7, EsTestUtils.DATASET_TEST_8, EsTestUtils.DATASET_TEST_9);
    EsTestUtils.indexDatasets(ES_SERVER, independentDatasets, attempt, null, alias);

    // Should
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET * 9,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), alias));
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET * 3,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), defIndex));
    Assert.assertEquals(
        EsTestUtils.DEFAULT_REC_DATASET * 3,
        EsIndex.countDocuments(ES_SERVER.getEsConfig(), dynIndex));

    List<String> allDatasets = new ArrayList<>();
    allDatasets.addAll(staticDatasets);
    allDatasets.addAll(dynamicDatasets);
    allDatasets.addAll(independentDatasets);

    // assert number of records for each dataset, it should remain as in the beginning
    allDatasets.forEach(
        d ->
            Assert.assertEquals(
                EsTestUtils.DEFAULT_REC_DATASET,
                EsTestUtils.countDocumentsFromQuery(
                    ES_SERVER, alias, String.format(EsTestUtils.MATCH_QUERY, "datasetKey", d))));

    // When
    // 2. Switch some datasets to other indexes without changing the number of records
    attempt++;
    switchDatasetAndRecords(
        Arrays.asList(EsTestUtils.DATASET_TEST, EsTestUtils.DATASET_TEST_9),
        dynIndex,
        alias,
        attempt,
        0);
    switchDatasetAndRecords(
        Arrays.asList(EsTestUtils.DATASET_TEST_4, EsTestUtils.DATASET_TEST_7),
        defIndex,
        alias,
        attempt,
        0);
    switchDatasetAndRecords(
        EsTestUtils.DATASET_TEST_2, EsTestUtils.DATASET_TEST_2 + "_" + attempt, alias, attempt, 0);

    // assert number of records for each dataset, it should remain as in the beginning
    allDatasets.forEach(
        d ->
            Assert.assertEquals(
                EsTestUtils.DEFAULT_REC_DATASET,
                EsTestUtils.countDocumentsFromQuery(
                    ES_SERVER, alias, String.format(EsTestUtils.MATCH_QUERY, "datasetKey", d))));

    // 3. Switch some datasets again but adding and deleting records.
    attempt++;
    switchDatasetAndRecords(EsTestUtils.DATASET_TEST, defIndex, alias, attempt, 5);
    switchDatasetAndRecords(
        EsTestUtils.DATASET_TEST_3, EsTestUtils.DATASET_TEST_3 + "_" + attempt, alias, attempt, 2);
    switchDatasetAndRecords(EsTestUtils.DATASET_TEST_4, dynIndex, alias, attempt, -7);
    switchDatasetAndRecords(EsTestUtils.DATASET_TEST_8, dynIndex, alias, attempt, -1);
  }

  private void switchDatasetAndRecords(
      List<String> datasets, String targetIdx, String alias, int attempt, int diffRecords) {
    datasets.forEach(
        datasetKey -> {
          // there should be only one source index
          String sourceIdx =
              EsIndex.findDatasetIndexesInAliases(
                      ES_SERVER.getEsConfig(), new String[] {alias}, datasetKey)
                  .iterator()
                  .next();

          final long previousCountAlias = EsIndex.countDocuments(ES_SERVER.getEsConfig(), alias);
          final long previousSourceCount =
              !sourceIdx.startsWith(datasetKey)
                  ? EsIndex.countDocuments(ES_SERVER.getEsConfig(), sourceIdx)
                  : 0;
          final long previousTargetCount =
              !targetIdx.startsWith(datasetKey)
                  ? EsIndex.countDocuments(ES_SERVER.getEsConfig(), targetIdx)
                  : 0;
          final long previousRecordsDataset =
              EsTestUtils.countDocumentsFromQuery(
                  ES_SERVER,
                  alias,
                  String.format(EsTestUtils.MATCH_QUERY, "datasetKey", datasetKey));
          final long recordsDataset = previousRecordsDataset + diffRecords;

          // When
          EsTestUtils.indexDatasets(
              ES_SERVER,
              Collections.singletonList(datasetKey),
              attempt,
              targetIdx,
              alias,
              recordsDataset);

          // Should
          assertEquals(
              previousCountAlias + diffRecords,
              EsIndex.countDocuments(ES_SERVER.getEsConfig(), alias));
          assertEquals(
              previousTargetCount + recordsDataset,
              EsIndex.countDocuments(ES_SERVER.getEsConfig(), targetIdx));

          if (sourceIdx.startsWith(datasetKey)) {
            assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), sourceIdx));
          } else {
            assertEquals(
                previousSourceCount - previousRecordsDataset,
                EsIndex.countDocuments(ES_SERVER.getEsConfig(), sourceIdx));
          }
          Assert.assertEquals(
              recordsDataset,
              EsTestUtils.countDocumentsFromQuery(
                  ES_SERVER,
                  alias,
                  String.format(EsTestUtils.MATCH_QUERY, "datasetKey", datasetKey)));
        });
  }

  private void switchDatasetAndRecords(
      String datasetKey, String targetIdx, String alias, int attempt, int diffRecords) {
    switchDatasetAndRecords(
        Collections.singletonList(datasetKey), targetIdx, alias, attempt, diffRecords);
  }
}
