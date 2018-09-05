package org.gbif.pipelines.minipipelines;

import org.gbif.pipelines.base.options.IndexingPipelineOptions;
import org.gbif.pipelines.base.pipelines.IndexingWithCreationPipeline;
import org.gbif.pipelines.base.transforms.MapTransforms;
import org.gbif.pipelines.base.transforms.RecordTransforms;
import org.gbif.pipelines.base.transforms.UniqueIdTransform;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.nio.file.Paths;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
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

/** TODO: DOC! */
class DwcaIndexingPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaIndexingPipeline.class);

  private final IndexingPipelineOptions options;

  private DwcaIndexingPipeline(IndexingPipelineOptions options) {
    this.options = options;
  }

  static DwcaIndexingPipeline create(IndexingPipelineOptions options) {
    return new DwcaIndexingPipeline(options);
  }

  /** TODO: DOC! */
  void run() {
    IndexingWithCreationPipeline.create(options).run(this::runPipeline);
  }

  /** TODO: DOC! */
  void runPipeline() {

    LOG.info("Adding step 1: Options");

    String wsProperties = options.getWsProperties();

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
        isDirectory ? DwcaIO.Read.withPaths(inputPath) : DwcaIO.Read.withPaths(inputPath, tmpDir);

    Pipeline p = Pipeline.create(options);

    LOG.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", reader)
            .apply("Filter duplicates", UniqueIdTransform.create());

    LOG.info("Adding step 2: Reading avros");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", RecordTransforms.metadata(wsProperties))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        uniqueRecords.apply("Map Verbatim to KV", MapTransforms.extendedToKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        uniqueRecords
            .apply("Interpret basic", RecordTransforms.basic())
            .apply("Map Basic to KV", MapTransforms.basicToKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        uniqueRecords
            .apply("Interpret temporal", RecordTransforms.temporal())
            .apply("Map Temporal to KV", MapTransforms.temporalToKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        uniqueRecords
            .apply("Interpret location", RecordTransforms.location(wsProperties))
            .apply("Map Location to KV", MapTransforms.locationToKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        uniqueRecords
            .apply("Interpret taxonomy", RecordTransforms.taxonomy(wsProperties))
            .apply("Map Taxon to KV", MapTransforms.taxonToKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        uniqueRecords
            .apply("Interpret multimedia", RecordTransforms.multimedia())
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
            options.getEsHosts(), options.getEsIndexName(), "record");

    jsonCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withMaxBatchSize(options.getEsMaxBatchSize()));

    LOG.info("Running indexing pipeline");
    p.run().waitUntilFinish();
  }
}
