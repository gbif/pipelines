package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.IndexingPipelineOptions;
import org.gbif.pipelines.base.options.PipelinesOptionsFactory;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.base.utils.IndexingUtils;

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
 * java -cp target/base-0.1-SNAPSHOT-shaded.jar org.gbif.pipelines.base.pipelines.IndexingWithCreationPipeline examples/configs/indexing.creation.properties
 *
 * or pass all parameters:
 *
 * java -cp target/base-0.1-SNAPSHOT-shaded.jar org.gbif.pipelines.base.pipelines.IndexingWithCreationPipeline
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
public class IndexingWithCreationPipeline {

  private IndexingWithCreationPipeline() {}

  public static void main(String[] args) {
    IndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    IndexingWithCreationPipeline.createAndRun(options);
  }

  public static void createAndRun(IndexingPipelineOptions options) {

    IndexingUtils.createIndex(options);

    IndexingPipeline.createAndRun(options);

    IndexingUtils.swapIndex(options);

    FsUtils.removeTmpDirecrory(options);
  }
}
