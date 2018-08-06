package org.gbif.pipelines.indexing;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.transform.indexing.MergeRecords2JsonTransform;
import org.gbif.pipelines.utils.FsUtils;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingPipelineBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingPipelineBuilder.class);

  public static void main(String[] args) {

    LOG.info("Starting indexing pipeline");

    EsProcessingPipelineOptions options = DataPipelineOptionsFactory.createForEs(args);

    buildPipeline(options).run().waitUntilFinish();
  }

  static Pipeline buildPipeline(EsProcessingPipelineOptions options) {

    LOG.info("Adding step 1: Options");
    final String pathIn =
        FsUtils.buildPathString(
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString());

    final String pathVerbatim = pathIn + "/verbatim.avro";
    final String pathMetadata = pathIn + "/metadata.avro";
    final String pathCommon = pathIn + "/common/interpreted*.avro";
    final String pathTemporal = pathIn + "/temporal/interpreted*.avro";
    final String pathLocation = pathIn + "/location/interpreted*.avro";
    final String pathTaxonomy = pathIn + "/taxonomy/interpreted*.avro";
    final String pathMultimedia = pathIn + "/multimedia/interpreted*.avro";

    Pipeline pipeline = Pipeline.create(options);
    Coders.registerAvroCoders(
        pipeline,
        InterpretedExtendedRecord.class,
        LocationRecord.class,
        TemporalRecord.class,
        TaxonRecord.class,
        MultimediaRecord.class,
        ExtendedRecord.class,
        MetadataRecord.class);

    LOG.info("Adding step 2: Reading avros");
    PCollection<KV<String, MetadataRecord>> metadataCollection =
        pipeline
            .apply("Read METADATA", AvroIO.read(MetadataRecord.class).from(pathMetadata))
            .apply(
                "Map METADATA",
                MapElements.into(new TypeDescriptor<KV<String, MetadataRecord>>() {})
                    .via((MetadataRecord md) -> KV.of(md.getDatasetId(), md)));

    PCollection<ExtendedRecord> verbatimCollection =
        pipeline.apply("Read VERBATIM", AvroIO.read(ExtendedRecord.class).from(pathVerbatim));

    PCollection<KV<String, InterpretedExtendedRecord>> interpretedRecordCollection =
        pipeline
            .apply("Read COMMON", AvroIO.read(InterpretedExtendedRecord.class).from(pathCommon))
            .apply(
                "Map COMMON",
                MapElements.into(new TypeDescriptor<KV<String, InterpretedExtendedRecord>>() {})
                    .via((InterpretedExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        pipeline
            .apply("Read TEMPORAL", AvroIO.read(TemporalRecord.class).from(pathTemporal))
            .apply(
                "Map TEMPORAL",
                MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
                    .via((TemporalRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, LocationRecord>> locationCollection =
        pipeline
            .apply("Read LOCATION", AvroIO.read(LocationRecord.class).from(pathLocation))
            .apply(
                "Map LOCATION",
                MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
                    .via((LocationRecord l) -> KV.of(l.getId(), l)));

    PCollection<KV<String, TaxonRecord>> taxonomyCollection =
        pipeline
            .apply("Read TAXONOMY", AvroIO.read(TaxonRecord.class).from(pathTaxonomy))
            .apply(
                "Map TAXONOMY",
                MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
                    .via((TaxonRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        pipeline
            .apply("Read MULTIMEDIA", AvroIO.read(MultimediaRecord.class).from(pathMultimedia))
            .apply(
                "Map MULTIMEDIA",
                MapElements.into(new TypeDescriptor<KV<String, MultimediaRecord>>() {})
                    .via((MultimediaRecord m) -> KV.of(m.getId(), m)));

    LOG.info("Adding step 3: Converting to a json object");
    MergeRecords2JsonTransform jsonTransform =
        MergeRecords2JsonTransform.create().withAvroCoders(pipeline);
    PCollectionTuple tuple =
        PCollectionTuple.of(jsonTransform.getExtendedRecordTag(), verbatimCollection)
            .and(jsonTransform.getInterKvTag(), interpretedRecordCollection)
            .and(jsonTransform.getLocationKvTag(), locationCollection)
            .and(jsonTransform.getMultimediaKvTag(), multimediaCollection)
            .and(jsonTransform.getTaxonomyKvTag(), taxonomyCollection)
            .and(jsonTransform.getTemporalKvTag(), temporalCollection)
            .and(jsonTransform.getMetadataKvTag(), metadataCollection);

    PCollection<String> resultCollection = tuple.apply("Merge object to Json", jsonTransform);

    LOG.info("Adding step 4: Elasticsearch configuration");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getESHosts(), options.getESIndexName(), "record");

    resultCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getESMaxBatchSizeBytes())
            .withMaxBatchSize(options.getESMaxBatchSize()));

    return pipeline;
  }
}
