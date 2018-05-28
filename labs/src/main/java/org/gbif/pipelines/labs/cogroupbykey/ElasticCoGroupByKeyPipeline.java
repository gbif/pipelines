package org.gbif.pipelines.labs.cogroupbykey;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.labs.mapper.ExtendedOccurrenceMapper;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticCoGroupByKeyPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticCoGroupByKeyPipeline.class);

  public static void main(String... args) {
    avrosToEs(args);
  }

  public static void avrosToEs(String... args) {

    LOG.info("Starting indexing pipeline with options - {}", Arrays.toString(args));

    LOG.info("Added step 0: Creating pipeline options");
    final TupleTag<InterpretedExtendedRecord> extendedRecordTag = new TupleTag<InterpretedExtendedRecord>() {};
    final TupleTag<TemporalRecord> temporalTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<Location> locationTag = new TupleTag<Location>() {};
    final TupleTag<TaxonRecord> taxonomyTag = new TupleTag<TaxonRecord>() {};
    final TupleTag<MultimediaRecord> multimediaTag = new TupleTag<MultimediaRecord>() {};

    EsProcessingPipelineOptions options = DataPipelineOptionsFactory.createForEs(args);

    final String pathIn = options.getInputFile();

    final String pathCommon = pathIn + "common/interpreted*.avro";
    final String pathTemporal = pathIn + "temporal/interpreted*.avro";
    final String pathLocation = pathIn + "location/interpreted*.avro";
    final String pathTaxonomy = pathIn + "taxonomy/interpreted*.avro";
    final String pathMultimedia = pathIn + "multimedia/interpreted*.avro";

    Pipeline p = Pipeline.create(options);
    Coders.registerAvroCoders(p, InterpretedExtendedRecord.class, Location.class, TemporalRecord.class);
    Coders.registerAvroCoders(p, TaxonRecord.class, MultimediaRecord.class);

    LOG.info("Adding step 1: Reading interpreted avro files");
    PCollection<KV<String, InterpretedExtendedRecord>> extendedRecordCollection =
      p.apply("Read COMMON", AvroIO.read(InterpretedExtendedRecord.class).from(pathCommon))
        .apply("Map COMMON", MapElements.into(new TypeDescriptor<KV<String, InterpretedExtendedRecord>>() {})
                 .via((InterpretedExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    PCollection<KV<String, TemporalRecord>> temporalCollection =
      p.apply("Read TEMPORAL", AvroIO.read(TemporalRecord.class).from(pathTemporal))
        .apply("Map TEMPORAL", MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
                 .via((TemporalRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, Location>> locationCollection =
      p.apply("Read LOCATION", AvroIO.read(Location.class).from(pathLocation))
        .apply("Map LOCATION", MapElements.into(new TypeDescriptor<KV<String, Location>>() {})
                 .via((Location l) -> KV.of(l.getOccurrenceID(), l)));

    PCollection<KV<String, TaxonRecord>> taxonomyCollection =
      p.apply("Read TAXONOMY", AvroIO.read(TaxonRecord.class).from(pathTaxonomy))
        .apply("Map TAXONOMY", MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
                 .via((TaxonRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
      p.apply("Read MULTIMEDIA", AvroIO.read(MultimediaRecord.class).from(pathMultimedia))
        .apply("Map MULTIMEDIA", MapElements.into(new TypeDescriptor<KV<String, MultimediaRecord>>() {})
                 .via((MultimediaRecord m) -> KV.of(m.getId(), m)));

    LOG.info("Adding step 2: Grouping by occurrenceID key");
    PCollection<KV<String, CoGbkResult>> groupedCollection =
      KeyedPCollectionTuple.of(extendedRecordTag, extendedRecordCollection)
        .and(temporalTag, temporalCollection)
        .and(locationTag, locationCollection)
        .and(taxonomyTag, taxonomyCollection)
        .and(multimediaTag, multimediaCollection)
        .apply(CoGroupByKey.create());

    LOG.info("Adding step 3: Converting to a flat object");
    PCollection<String> resultCollection = groupedCollection.apply("Merge objects", ParDo.of(
      new DoFn<KV<String, CoGbkResult>, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          CoGbkResult value = c.element().getValue();
          InterpretedExtendedRecord extendedRecord = value.getOnly(extendedRecordTag);
          TemporalRecord temporal = value.getOnly(temporalTag);
          Location location = value.getOnly(locationTag);
          TaxonRecord taxon = value.getOnly(taxonomyTag);
          MultimediaRecord multimedia = value.getOnly(multimediaTag);
          c.output(ExtendedOccurrenceMapper.map(extendedRecord, location, temporal, taxon, multimedia).toString());
        }
      }
    ));


    LOG.info("Adding step 4: Elasticsearch configuration");
    final String[] esHost = options.getESHosts();
    final String esIndex = options.getESIndex();
    final String esType = options.getESType();
    final Integer batchSize = options.getESMaxBatchSize();

    ElasticsearchIO.ConnectionConfiguration esConfig = ElasticsearchIO.ConnectionConfiguration.create(esHost, esIndex, esType);
    resultCollection.apply(ElasticsearchIO.write().withConnectionConfiguration(esConfig).withMaxBatchSize(batchSize));

    LOG.info("Run the pipeline");
    p.run().waitUntilFinish();

  }

}

