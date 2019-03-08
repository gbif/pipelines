package org.gbif.pipelines.ingest.pipelines;

import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.transforms.extension.AmplificationTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

/**
 * NOTE: REMEMBER THAT THIS PIPELINE MUST BE STARTED AFTER {@link InterpretedToEsIndexPipeline}
 * <p>
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files {@link org.gbif.pipelines.io.avro.AmplificationRecord}
 *    3) Converts to json model (resources/elasticsearch/es-occurrence-schema.json)
 *    4) Updates existing ES index
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.base.pipelines.InterpretedToEsIndexAmpPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.base.pipelines.InterpretedToEsIndexAmpPipeline
 * --datasetId=9f747cff-839f-4485-83a1-f10317a92a82
 * --attempt=1
 * --runner=SparkRunner
 * --targetPath=hdfs://ha-nn/output/
 * --esIndexName=pipeline
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200
 * --hdfsSiteConfig=/config/hdfs-site.xml
 * --coreSiteConfig=/config/core-site.xml
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToEsIndexAmpPipeline {

  public static void main(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpret(options, t, "*" + AVRO_EXTENSION);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Reading avros and converting into a json object");
    PCollection<String> jsonCollection =
        p.apply("Read Amplification", AmplificationTransform.read(pathFn))
            .apply("Merging into json", ParDo.of(new DoFn<AmplificationRecord, String>() {

              private final Counter counter = Metrics.counter(GbifJsonConverter.class, AVRO_TO_JSON_COUNT);

              @ProcessElement
              public void processElement(ProcessContext c) {
                String json = GbifJsonConverter.createWithIdAndSkipIssues(c.element()).buildJson().toString();
                c.output(json);
                counter.inc();
              }
            }));

    log.info("Adding step 4: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), Indexing.INDEX_TYPE);

    jsonCollection.apply(
        ElasticsearchIO.write()
            .withUsePartialUpdate(true)
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withMaxBatchSize(options.getEsMaxBatchSize())
            .withIdFn(input -> input.get("id").asText()));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Pipeline has been finished");
  }
}
