package org.gbif.pipelines.ingest.pipelines;

import org.gbif.pipelines.core.RecordType;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.ingest.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.MapTransforms;
import org.gbif.pipelines.transforms.ReadTransforms;

import java.util.function.Function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
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

import static org.gbif.pipelines.core.RecordType.BASIC;
import static org.gbif.pipelines.core.RecordType.LOCATION;
import static org.gbif.pipelines.core.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.core.RecordType.TAXONOMY;
import static org.gbif.pipelines.core.RecordType.TEMPORAL;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads {@link org.gbif.pipelines.io.avro.MetadataRecord}, {@link org.gbif.pipelines.io.avro.BasicRecord},
 *        {@link org.gbif.pipelines.io.avro.TemporalRecord}, {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *        {@link org.gbif.pipelines.io.avro.TaxonRecord}, {@link org.gbif.pipelines.io.avro.LocationRecord} avro files
 *    2) Joins avro files
 *    3) Converts to json model (resources/elasticsearch/es-occurrence-shcema.json)
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
public class InterpretedToEsIndexPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretedToEsIndexPipeline.class);

  private InterpretedToEsIndexPipeline() {}

  public static void main(String[] args) {
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    createAndRun(options);
  }

  public static void createAndRun(EsIndexingPipelineOptions options) {
    LOG.info("Running indexing pipeline");
    InterpretedToEsIndexPipeline.create(options).run().waitUntilFinish();
    LOG.info("Indexing pipeline has been finished");
  }

  public static Pipeline create(EsIndexingPipelineOptions options) {

    LOG.info("Adding step 1: Options");
    Function<String, String> pathFn = s -> FsUtils.buildPath(options, s + "*.avro");
    Function<RecordType, String> pathInterFn =
        t -> FsUtils.buildPathInterpret(options, t.name().toLowerCase(), "*.avro");

    final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
    final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
    final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
    final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
    final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};

    Pipeline p = Pipeline.create(options);

    LOG.info("Adding step 2: Reading avros");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", ReadTransforms.metadata(pathFn.apply("metadata")))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", ReadTransforms.extended(pathFn.apply("verbatim")))
            .apply("Map Verbatim to KV", MapTransforms.extendedToKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", ReadTransforms.basic(pathInterFn.apply(BASIC)))
            .apply("Map Basic to KV", MapTransforms.basicToKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", ReadTransforms.temporal(pathInterFn.apply(TEMPORAL)))
            .apply("Map Temporal to KV", MapTransforms.temporalToKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", ReadTransforms.location(pathInterFn.apply(LOCATION)))
            .apply("Map Location to KV", MapTransforms.locationToKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("Read Taxon", ReadTransforms.taxon(pathInterFn.apply(TAXONOMY)))
            .apply("Map Taxon to KV", MapTransforms.taxonToKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", ReadTransforms.multimedia(pathInterFn.apply(MULTIMEDIA)))
            .apply("Map Multimedia to KV", MapTransforms.multimediaToKv());

    LOG.info("Adding step 3: Converting to a json object");
    DoFn<KV<String, CoGbkResult>, String> doFn =
        new DoFn<KV<String, CoGbkResult>, String>() {
          @ProcessElement
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

            String json =
                GbifJsonConverter.create(mdr, br, tr, lr, txr, mr, er).buildJson().toString();

            c.output(json);
          }
        };

    PCollection<String> jsonCollection =
        KeyedPCollectionTuple.of(brTag, basicCollection)
            .and(trTag, temporalCollection)
            .and(lrTag, locationCollection)
            .and(txrTag, taxonCollection)
            .and(mrTag, multimediaCollection)
            .and(erTag, verbatimCollection)
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", ParDo.of(doFn).withSideInputs(metadataView));

    LOG.info("Adding step 4: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), "record");

    jsonCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withMaxBatchSize(options.getEsMaxBatchSize()));

    return p;
  }
}
