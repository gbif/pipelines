package org.gbif.pipelines.ingest.pipelines;

import java.nio.file.Paths;
import java.util.Optional;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.EsIndexUtils;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.ws.config.WsConfig;
import org.gbif.pipelines.parsers.ws.config.WsConfigFactory;
import org.gbif.pipelines.transforms.MapTransforms;
import org.gbif.pipelines.transforms.UniqueIdTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.transforms.RecordTransforms.BasicFn;
import static org.gbif.pipelines.transforms.RecordTransforms.LocationFn;
import static org.gbif.pipelines.transforms.RecordTransforms.MetadataFn;
import static org.gbif.pipelines.transforms.RecordTransforms.MultimediaFn;
import static org.gbif.pipelines.transforms.RecordTransforms.TaxonomyFn;
import static org.gbif.pipelines.transforms.RecordTransforms.TemporalFn;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads DwCA archive and converts to {@link org.gbif.pipelines.io.avro.ExtendedRecord}
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file
 *        to {@link org.gbif.pipelines.io.avro.MetadataRecord}, {@link
 *        org.gbif.pipelines.io.avro.BasicRecord}, {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *        {@link org.gbif.pipelines.io.avro.MultimediaRecord}, {@link
 *        org.gbif.pipelines.io.avro.TaxonRecord}, {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    3) Joins objects
 *    4) Converts to json model (resources/elasticsearch/es-occurrence-shcema.json)
 *    5) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToEsIndexPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToEsIndexPipeline
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --inputPath=/some/path/to/input/dwca.zip
 * --esHosts=http://ADDRESS,http://ADDRESS,http://ADDRESS:9200
 * --esIndexName=pipeline
 *
 * }</pre>
 */
public class DwcaToEsIndexPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToEsIndexPipeline.class);

  private DwcaToEsIndexPipeline() {}

  public static void main(String[] args) {
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);
    run(options);
  }

  public static void run(DwcaPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    EsIndexUtils.createIndex(options);

    LOG.info("Adding step 1: Options");
    WsConfig wsConfig = WsConfigFactory.create(options.getGbifApiUrl());

    final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
    final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
    final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
    final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
    final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};

    String tmpDir = FsUtils.getTempDir(options);

    String inputPath = options.getInputPath();
    boolean isDirectory = Paths.get(inputPath).toFile().isDirectory();

    DwcaIO.Read reader =
        isDirectory
            ? DwcaIO.Read.fromLocation(inputPath)
            : DwcaIO.Read.fromCompressed(inputPath, tmpDir);

    Pipeline p = Pipeline.create(options);

    LOG.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", reader)
            .apply("Filter duplicates", UniqueIdTransform.create());

    LOG.info("Adding step 2: Reading avros");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", ParDo.of(new MetadataFn(wsConfig)))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        uniqueRecords.apply("Map Verbatim to KV", MapTransforms.extendedToKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        uniqueRecords
            .apply("Interpret basic", ParDo.of(new BasicFn()))
            .apply("Map Basic to KV", MapTransforms.basicToKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        uniqueRecords
            .apply("Interpret temporal", ParDo.of(new TemporalFn()))
            .apply("Map Temporal to KV", MapTransforms.temporalToKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        uniqueRecords
            .apply("Interpret location", ParDo.of(new LocationFn(wsConfig)))
            .apply("Map Location to KV", MapTransforms.locationToKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        uniqueRecords
            .apply("Interpret taxonomy", ParDo.of(new TaxonomyFn(wsConfig)))
            .apply("Map Taxon to KV", MapTransforms.taxonToKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        uniqueRecords
            .apply("Interpret multimedia", ParDo.of(new MultimediaFn()))
            .apply("Map Multimedia to KV", MapTransforms.multimediaToKv());

    LOG.info("Adding step 3: Converting to a json object");
    DoFn<KV<String, CoGbkResult>, String> doFn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter = Metrics.counter(GbifJsonConverter.class, AVRO_TO_JSON_COUNT);

          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            MetadataRecord mdr = c.sideInput(metadataView);
            ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
            TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
            TaxonRecord txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
            MultimediaRecord mr = v.getOnly(mrTag, MultimediaRecord.newBuilder().setId(k).build());

            String json = GbifJsonConverter.create(mdr, br, tr, lr, txr, mr, er).buildJson().toString();

            c.output(json);

            counter.inc();
          }
        };

    LOG.info("Adding step 4: Converting to a json object");
    PCollection<String> jsonCollection =
        KeyedPCollectionTuple.of(brTag, basicCollection)
            .and(trTag, temporalCollection)
            .and(lrTag, locationCollection)
            .and(txrTag, taxonCollection)
            .and(mrTag, multimediaCollection)
            .and(erTag, verbatimCollection)
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", ParDo.of(doFn).withSideInputs(metadataView));

    LOG.info("Adding step 5: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), Indexing.INDEX_TYPE);

    jsonCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withMaxBatchSize(options.getEsMaxBatchSize()));

    LOG.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    Optional.ofNullable(options.getMetaFileName()).ifPresent(metadataName -> {
      String metadataPath = metadataName.isEmpty() ? "" : FsUtils.buildPath(options, metadataName);
      MetricsHandler.saveCountersToFile(options.getHdfsSiteConfig(), metadataPath, result);
    });

    LOG.info("Pipeline has been finished");

    FsUtils.removeTmpDirecrory(options);
  }
}
