package org.gbif.pipelines.ingest.java.pipelines;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.utils.FsUtils;
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
 * java target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline \
 * --datasetId=${UUID} \
 * --attempt=1 \
 * --runner=SparkRunner \
 * --metaFileName=occurrence-to-index.yml \
 * --inputPath=${OUT} \
 * --targetPath=${OUT} \
 * --esSchemaPath=/gbif/pipelines/ingest-gbif-beam/src/main/resources/elasticsearch/es-occurrence-schema.json \
 * --indexNumberShards=1 \
 * --indexNumberReplicas=0 \
 * --esDocumentId=id \
 * --esAlias=alias_example \
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200 \
 * --esIndexName=index_name_example \
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToEsIndexExtendedPipeline {

  public static void main(String[] args) {
    run(args);
  }

  public static void run(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(String[] args, ExecutorService executor) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    InterpretedToEsIndexExtendedPipeline.run(options, executor);
  }

  public static void run(EsIndexingPipelineOptions options, ExecutorService executor) {
    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline.run(
        options, () -> OccurrenceToEsIndexPipeline.run(options, executor));

    FsUtils.removeTmpDirectory(PathBuilder.getTempDir(options));
    log.info("Finished main indexing pipeline");
  }
}
