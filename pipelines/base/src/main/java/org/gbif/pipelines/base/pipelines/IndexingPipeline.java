package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.DataPipelineOptionsFactory;
import org.gbif.pipelines.base.options.EsProcessingPipelineOptions;
import org.gbif.pipelines.base.transforms.MapTransforms;
import org.gbif.pipelines.base.transforms.ReadTransforms;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
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

/** TODO: DOC! */
public class IndexingPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingPipeline.class);

  private final EsProcessingPipelineOptions options;

  private IndexingPipeline(EsProcessingPipelineOptions options) {
    this.options = options;
  }

  public static IndexingPipeline create(EsProcessingPipelineOptions options) {
    return new IndexingPipeline(options);
  }

  /** TODO: DOC! */
  public static void main(String[] args) {
    EsProcessingPipelineOptions options = DataPipelineOptionsFactory.createForEs(args);
    IndexingPipeline.create(options).run();
  }

  /** TODO: DOC! */
  public PipelineResult.State run() {

    LOG.info("Adding step 1: Options");

    final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
    final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
    final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
    final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
    final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};

    Pipeline p = Pipeline.create(options);

    LOG.info("Adding step 2: Reading avros");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", ReadTransforms.metadata(options))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", ReadTransforms.verbatim(options))
            .apply("Map Verbatim to KV", MapTransforms.extendedToKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", ReadTransforms.basic(options))
            .apply("Map Basic to KV", MapTransforms.basicToKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", ReadTransforms.temporal(options))
            .apply("Map Temporal to KV", MapTransforms.temporalToKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", ReadTransforms.location(options))
            .apply("Map Location to KV", MapTransforms.locationToKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("Read Taxon", ReadTransforms.taxon(options))
            .apply("Map Taxon to KV", MapTransforms.taxonToKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", ReadTransforms.multimedia(options))
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
            options.getESHosts(), options.getESIndexName(), "record");

    jsonCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getESMaxBatchSizeBytes())
            .withMaxBatchSize(options.getESMaxBatchSize()));

    LOG.info("Running the pipeline");
    return p.run().waitUntilFinish();
  }
}
