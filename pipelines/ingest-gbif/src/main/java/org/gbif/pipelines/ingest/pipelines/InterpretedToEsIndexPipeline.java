package org.gbif.pipelines.ingest.pipelines;

import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.MetadataTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.AustraliaSpatialTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    2) Joins avro files
 *    3) Converts to json model (resources/elasticsearch/es-occurrence-schema.json)
 *    4) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.base.pipelines.InterpretedToEsIndexPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.base.pipelines.InterpretedToEsIndexPipeline
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
public class InterpretedToEsIndexPipeline {

  public static void main(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    run(options);
  }

  /**
   * Determines if the record has been repatriated, i.e.: country != publishing Organization Country.
   */
  private static Optional<Boolean> isRepatriated(LocationRecord lr, MetadataRecord mr) {

    if (Objects.nonNull(lr) && Objects.nonNull(mr) && Objects.nonNull(lr.getCountry()) && Objects.nonNull(mr.getPublishingCountry())) {
      return Optional.of(lr.getCountry().equals(mr.getPublishingCountry()));
    }
    return Optional.empty();
  }

  public static void run(EsIndexingPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpret(options, t, "*" + AVRO_EXTENSION);

    // Core
    final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
    final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
    final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
    final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
    // Extension
    final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};
    final TupleTag<ImageRecord> irTag = new TupleTag<ImageRecord>() {};
    final TupleTag<AudubonRecord> arTag = new TupleTag<AudubonRecord>() {};
    final TupleTag<MeasurementOrFactRecord> mfrTag = new TupleTag<MeasurementOrFactRecord>() {};
    // Specific
    final TupleTag<AustraliaSpatialRecord> asrTag = new TupleTag<AustraliaSpatialRecord>() {};

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Reading avros");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", MetadataTransform.read(pathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", VerbatimTransform.read(pathFn))
            .apply("Map Verbatim to KV", VerbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", BasicTransform.read(pathFn))
            .apply("Map Basic to KV", BasicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", TemporalTransform.read(pathFn))
            .apply("Map Temporal to KV", TemporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", LocationTransform.read(pathFn))
            .apply("Map Location to KV", LocationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("Read Taxon", TaxonomyTransform.read(pathFn))
            .apply("Map Taxon to KV", TaxonomyTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", MultimediaTransform.read(pathFn))
            .apply("Map Multimedia to KV", MultimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", ImageTransform.read(pathFn))
            .apply("Map Image to KV", ImageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", AudubonTransform.read(pathFn))
            .apply("Map Audubon to KV", AudubonTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementCollection =
        p.apply("Read Measurement", MeasurementOrFactTransform.read(pathFn))
            .apply("Map Measurement to KV", MeasurementOrFactTransform.toKv());

    PCollection<KV<String, AustraliaSpatialRecord>> australiaSpatialCollection =
        p.apply("Read Australia spatial", AustraliaSpatialTransform.read(pathFn))
            .apply("Map Australia spatial to KV", AustraliaSpatialTransform.toKv());

    log.info("Adding step 3: Converting into a json object");
    DoFn<KV<String, CoGbkResult>, String> doFn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter = Metrics.counter(GbifJsonConverter.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
            TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
            TaxonRecord txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
            // Extension
            MultimediaRecord mr = v.getOnly(mrTag, MultimediaRecord.newBuilder().setId(k).build());
            ImageRecord ir = v.getOnly(irTag, ImageRecord.newBuilder().setId(k).build());
            AudubonRecord ar = v.getOnly(arTag, AudubonRecord.newBuilder().setId(k).build());
            MeasurementOrFactRecord mfr = v.getOnly(mfrTag, MeasurementOrFactRecord.newBuilder().setId(k).build());
            // Specific
            AustraliaSpatialRecord asr = v.getOnly(asrTag, AustraliaSpatialRecord.newBuilder().setId(k).build());

            MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

            //Interpretation that required multiple sources
            isRepatriated(lr, mdr).ifPresent(lr::setRepatriated);

            String json = GbifJsonConverter.toStringJson(mdr, br, tr, lr, txr, mmr, mfr, er, asr);

            c.output(json);

            counter.inc();
          }
        };

    PCollection<String> jsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(brTag, basicCollection)
            .and(trTag, temporalCollection)
            .and(lrTag, locationCollection)
            .and(txrTag, taxonCollection)
            // Extension
            .and(mrTag, multimediaCollection)
            .and(irTag, imageCollection)
            .and(arTag, audubonCollection)
            .and(mfrTag, measurementCollection)
            // Specific
            .and(asrTag, australiaSpatialCollection)
            // Raw
            .and(erTag, verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", ParDo.of(doFn).withSideInputs(metadataView));

    log.info("Adding step 4: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), Indexing.INDEX_TYPE);

    jsonCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withMaxBatchSize(options.getEsMaxBatchSize())
            .withIdFn(input -> input.get("id").asText()));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    Optional.ofNullable(options.getMetaFileName()).ifPresent(metadataName -> {
      String metadataPath = metadataName.isEmpty() ? "" : FsUtils.buildPath(options, metadataName);
      MetricsHandler.saveCountersToFile(options.getHdfsSiteConfig(), metadataPath, result);
    });

    log.info("Pipeline has been finished");
  }
}
