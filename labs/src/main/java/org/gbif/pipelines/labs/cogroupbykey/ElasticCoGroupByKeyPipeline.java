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

/**
 *
 * PLEASE READ ES-CO-GROUP-BY-KEY.MD FILE
 *
 * How to run example:
 *
 * sudo -u hdfs spark2-submit --conf spark.default.parallelism=100 --conf
 * spark.executor.memoryOverhead=2048 --class
 * org.gbif.pipelines.labs.cogroupbykey.ElasticCoGroupByKeyPipeline --master yarn --deploy-mode
 * cluster --executor-memory 12G --executor-cores 8 --num-executors 16 --driver-memory 1G
 * /home/crap/lib/labs-1.1-SNAPSHOT-shaded.jar --datasetId=0645ccdb-e001-4ab0-9729-51f1755e007e
 * --attempt=1 --runner=SparkRunner
 * --defaultTargetDirectory=hdfs://ha-nn/data/ingest/0645ccdb-e001-4ab0-9729-51f1755e007e/1/
 * --inputFile=hdfs://ha-nn/data/ingest/0645ccdb-e001-4ab0-9729-51f1755e007e/1/
 * --hdfsSiteConfig=/home/crap/config/hdfs-site.xml --coreSiteConfig=/home/crap/config/core-site.xml
 * --ESHosts=http://c3n1.gbif.org:9200,http://c3n2.gbif.org:9200,http://c3n3.gbif.org:9200
 * --ESIndex=co-group-idx --ESType=co-group-idx --ESMaxBatchSize=1000
 */
public class ElasticCoGroupByKeyPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticCoGroupByKeyPipeline.class);

  public static void main(String... args) {
    avrosToEs(args);
  }

  public static void avrosToEs(String... args) {
    EsProcessingPipelineOptions options = DataPipelineOptionsFactory.createForEs(args);
    avrosToEs(options);
  }

  public static void avrosToEs(EsProcessingPipelineOptions options) {

    LOG.info("Starting indexing pipeline");

    LOG.info("Added step 0: Creating pipeline options");
    final TupleTag<InterpretedExtendedRecord> extendedRecordTag = new TupleTag<InterpretedExtendedRecord>() {};
    final TupleTag<TemporalRecord> temporalTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<Location> locationTag = new TupleTag<Location>() {};
    final TupleTag<TaxonRecord> taxonomyTag = new TupleTag<TaxonRecord>() {};
    final TupleTag<MultimediaRecord> multimediaTag = new TupleTag<MultimediaRecord>() {};

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

    LOG.info("Adding step 2: Grouping by id/occurrenceID key");
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
          InterpretedExtendedRecord extendedRecord = value.getOnly(extendedRecordTag, InterpretedExtendedRecord.newBuilder().setId("").build());
          TemporalRecord temporal = value.getOnly(temporalTag, TemporalRecord.newBuilder().setId("").build());
          Location location = value.getOnly(locationTag, Location.newBuilder().setOccurrenceID("").build());
          TaxonRecord taxon = value.getOnly(taxonomyTag, TaxonRecord.newBuilder().setId("").build());
          MultimediaRecord multimedia = value.getOnly(multimediaTag, MultimediaRecord.newBuilder().setId("").build());
          c.output(ExtendedOccurrenceMapper.map(extendedRecord, location, temporal, taxon, multimedia).toString());
        }
      }
    ));


    LOG.info("Adding step 4: Elasticsearch configuration");
    ElasticsearchIO.ConnectionConfiguration esConfig = ElasticsearchIO.ConnectionConfiguration.create(
            options.getESHosts(), options.getESIndex(), options.getESType());

    resultCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getESMaxBatchSize())
            .withMaxBatchSize(options.getESMaxBatchSizeBytes()));

    LOG.info("Run the pipeline");
    p.run().waitUntilFinish();

  }

}

