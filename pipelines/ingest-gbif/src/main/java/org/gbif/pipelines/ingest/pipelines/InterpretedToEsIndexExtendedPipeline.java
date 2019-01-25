package org.gbif.pipelines.ingest.pipelines;

import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.EsIndexUtils;
import org.gbif.pipelines.ingest.utils.FsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Creates an Elasticsearch index
 *    2) Reads {@link org.gbif.pipelines.io.avro.MetadataRecord}, {@link org.gbif.pipelines.io.avro.BasicRecord},
 *        {@link org.gbif.pipelines.io.avro.TemporalRecord}, {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *        {@link org.gbif.pipelines.io.avro.TaxonRecord}, {@link org.gbif.pipelines.io.avro.LocationRecord} avro files
 *    3) Joins avro files
 *    4) Converts to json model (resources/elasticsearch/es-occurrence-shcema.json)
 *    5) Pushes data to Elasticsearch instance
 *    6) Swaps index name and index alias
 *    7) Deletes temporal files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline
 * --datasetId=9f747cff-839f-4485-83a1-f10317a92a82
 * --attempt=1
 * --runner=SparkRunner
 * --targetPath=hdfs://ha-nn/output/
 * --esAlias=pipeline
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200
 * --hdfsSiteConfig=/config/hdfs-site.xml
 * --coreSiteConfig=/config/core-site.xml
 *
 * }</pre>
 */
public class InterpretedToEsIndexExtendedPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretedToEsIndexExtendedPipeline.class);

  private InterpretedToEsIndexExtendedPipeline() {}

  public static void main(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    InterpretedToEsIndexExtendedPipeline.run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    EsIndexUtils.createIndexIfNotExist(options);
    EsIndexUtils.deleteRecordsByDatasetId(options);

    InterpretedToEsIndexPipeline.run(options);

    EsIndexUtils.swapIndexIfAliasExists(options);

    FsUtils.removeTmpDirectory(options);
    LOG.info("Finished main indexing pipeline");

    LOG.info("Call - System.exit(0)"); // Workaround for a bug with stuck job
    System.exit(0);
  }
}
