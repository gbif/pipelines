package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.EsIndexUtils;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Creates an Elasticsearch index
 *    2) Reads:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *      avro files
 *    3) Joins avro files
 *    4) Converts to json model (resources/elasticsearch/es-occurrence-schema.json)
 *    5) Pushes data to Elasticsearch instance
 *    6) Swaps index name and index alias
 *    7) Deletes temporal files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar \
 *  --pipelineStep=INTERPRETED_TO_ES_INDEX \
 *  --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 *  --attempt=1 \
 *  --runner=SparkRunner \
 *  --inputPath=/path \
 *  --targetPath=/path \
 *  --esIndexName=test2_java \
 *  --esAlias=occurrence2_java \
 *  --indexNumberShards=3 \
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200 \
 * --properties=/home/nvolik/Projects/GBIF/gbif-configuration/cli/dev/config/pipelines.properties \
 * --esDocumentId=id
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToEsIndexExtendedPipeline {

  public static void main(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    InterpretedToEsIndexExtendedPipeline.run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {
    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", options.getStepType().name());

    if (DatasetType.OCCURRENCE == options.getDatasetType()) {
      run(options, () -> OccurrenceToEsIndexPipeline.run(options));
    } else if (DatasetType.SAMPLING_EVENT == options.getDatasetType()) {
      run(options, () -> EventToEsIndexPipeline.run(options));
    } else {
      throw new IllegalArgumentException(
          "DatasetType" + options.getDatasetType() + " nor recognized for this pipeline");
    }

    FsUtils.removeTmpDirectoryAfterShutdown(PathBuilder.getTempDir(options));
    log.info("Finished main indexing pipeline");
  }

  public static void run(EsIndexingPipelineOptions options, Runnable pipeline) {
    EsIndexUtils.createIndexAndAliasForDefault(options);

    // Returns indices names in case of swapping
    Set<String> indices = EsIndexUtils.deleteRecordsByDatasetId(options);

    pipeline.run();

    PipelinesConfig config = null;
    if (options.getProperties() != null) {
      config =
          FsUtils.readConfigFile(
              HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
              options.getProperties(),
              PipelinesConfig.class);

      String zk = config.getIndexLock().getZkConnectionString();
      zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
      config.getIndexLock().setZkConnectionString(zk);
    } else {
      log.error("Pipelines properties is null! Check properties path!");
    }

    EsIndexUtils.updateAlias(options, indices, config != null ? config.getIndexLock() : null);
    EsIndexUtils.refreshIndex(options);

    saveMetrics(options);
  }

  /**
   * Query Elasticseach documnets count using dataset key as a filter and save as a meta metrics
   * file
   */
  private static void saveMetrics(EsIndexingPipelineOptions options) {

    long documentsCountByDatasetKey = EsIndexUtils.getDocumentsCountByDatasetKey(options);
    String metrics = AVRO_TO_JSON_COUNT + "Attempted: " + documentsCountByDatasetKey;

    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveMetricsToFile(options, metrics, false);
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    FsUtils.setOwner(
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
        metadataPath,
        "crap",
        "supergroup");
  }
}
