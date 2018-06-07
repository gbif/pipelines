package org.gbif.pipelines.labs.indexing;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

import java.util.function.BiConsumer;

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
 * PLEASE READ DOCS/ES-CO-GROUP-BY-KEY.MD FILE
 *
 * CoGrouping and indexing object with nested objects
 *
 * How to run example:
 *
 * sudo -u hdfs spark2-submit --conf spark.default.parallelism=100 --conf
 * spark.executor.memoryOverhead=4000 --class
 * org.gbif.pipelines.labs.indexing.EsCoGroupNestedPipeline --master yarn --deploy-mode
 * cluster --executor-memory 12G --executor-cores 8 --num-executors 16 --driver-memory 1G
 * /home/crap/lib/labs-1.1-SNAPSHOT-shaded.jar --datasetId=0645ccdb-e001-4ab0-9729-51f1755e007e
 * --attempt=1 --runner=SparkRunner
 * --defaultTargetDirectory=hdfs://ha-nn/data/ingest/0645ccdb-e001-4ab0-9729-51f1755e007e/1/
 * --inputFile=hdfs://ha-nn/data/ingest/0645ccdb-e001-4ab0-9729-51f1755e007e/1/
 * --hdfsSiteConfig=/home/crap/config/hdfs-site.xml --coreSiteConfig=/home/crap/config/core-site.xml
 * --ESAddresses=http://c3n1.gbif.org:9200,http://c3n2.gbif.org:9200,http://c3n3.gbif.org:9200
 * --ESIndexPrefix=co-group-idx --ESMaxBatchSize=1000
 */
public class EsCoGroupNestedPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(EsCoGroupNestedPipeline.class);

  public static void main(String... args) {
    avrosToEs(args);
  }

  public static void avrosToEs(String... args) {
    avrosToEs(DataPipelineOptionsFactory.createForEs(args));
  }

  public static void avrosToEs(EsProcessingPipelineOptions options) {

    LOG.info("Starting indexing pipeline");

    LOG.info("Added step 0: Creating pipeline options");
    final TupleTag<InterpretedExtendedRecord> interRecordTag = new TupleTag<InterpretedExtendedRecord>() {};
    final TupleTag<TemporalRecord> temporalTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<LocationRecord> locationTag = new TupleTag<LocationRecord>() {};
    final TupleTag<TaxonRecord> taxonomyTag = new TupleTag<TaxonRecord>() {};
    final TupleTag<MultimediaRecord> multimediaTag = new TupleTag<MultimediaRecord>() {};
    final TupleTag<ExtendedRecord> extendedRecordTag = new TupleTag<ExtendedRecord>() {};

    final String pathIn = options.getInputFile();

    final String pathCommon = pathIn + "common/interpreted*.avro";
    final String pathTemporal = pathIn + "temporal/interpreted*.avro";
    final String pathLocation = pathIn + "location/interpreted*.avro";
    final String pathTaxonomy = pathIn + "taxonomy/interpreted*.avro";
    final String pathMultimedia = pathIn + "multimedia/interpreted*.avro";
    final String pathExtended = pathIn + "verbatim*.avro";

    Pipeline p = Pipeline.create(options);
    Coders.registerAvroCoders(p, InterpretedExtendedRecord.class, LocationRecord.class, TemporalRecord.class);
    Coders.registerAvroCoders(p, TaxonRecord.class, MultimediaRecord.class, ExtendedRecord.class);

    LOG.info("Adding step 1: Reading interpreted avro files");
    PCollection<KV<String, ExtendedRecord>> extendedRecordCollection =
      p.apply("Read RAW", AvroIO.read(ExtendedRecord.class).from(pathExtended))
        .apply("Map RAW", MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
          .via((ExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    PCollection<KV<String, InterpretedExtendedRecord>> interpretedRecordCollection =
      p.apply("Read COMMON", AvroIO.read(InterpretedExtendedRecord.class).from(pathCommon))
        .apply("Map COMMON", MapElements.into(new TypeDescriptor<KV<String, InterpretedExtendedRecord>>() {})
          .via((InterpretedExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    PCollection<KV<String, TemporalRecord>> temporalCollection =
      p.apply("Read TEMPORAL", AvroIO.read(TemporalRecord.class).from(pathTemporal))
        .apply("Map TEMPORAL", MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
          .via((TemporalRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, LocationRecord>> locationCollection =
      p.apply("Read LOCATION", AvroIO.read(LocationRecord.class).from(pathLocation))
        .apply("Map LOCATION", MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
          .via((LocationRecord l) -> KV.of(l.getId(), l)));

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
      KeyedPCollectionTuple.of(interRecordTag, interpretedRecordCollection)
        .and(temporalTag, temporalCollection)
        .and(locationTag, locationCollection)
        .and(taxonomyTag, taxonomyCollection)
        .and(multimediaTag, multimediaCollection)
        .and(extendedRecordTag, extendedRecordCollection)
        .apply(CoGroupByKey.create());

    LOG.info("Adding step 3: Converting to a flat object");
    PCollection<String> resultCollection = groupedCollection.apply("Merge objects", ParDo.of(
      new DoFn<KV<String, CoGbkResult>, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          CoGbkResult value = c.element().getValue();
          String key = c.element().getKey();
          InterpretedExtendedRecord interRecord = value.getOnly(interRecordTag, InterpretedExtendedRecord.newBuilder().setId(key).build());
          TemporalRecord temporal = value.getOnly(temporalTag, TemporalRecord.newBuilder().setId(key).build());
          LocationRecord location = value.getOnly(locationTag, LocationRecord.newBuilder().setId(key).build());
          TaxonRecord taxon = value.getOnly(taxonomyTag, TaxonRecord.newBuilder().setId(key).build());
          MultimediaRecord multimedia = value.getOnly(multimediaTag, MultimediaRecord.newBuilder().setId(key).build());
          ExtendedRecord extendedRecord = value.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(key).build());
          c.output(toIndex(interRecord, temporal, location, taxon, multimedia, extendedRecord));
        }
      }
    ));

    LOG.info("Adding step 4: Elasticsearch configuration");
    ElasticsearchIO.ConnectionConfiguration esConfig = ElasticsearchIO.ConnectionConfiguration.create(
      options.getESAddresses(), options.getESIndexPrefix(), options.getESIndexPrefix());

    resultCollection.apply(
      ElasticsearchIO.write()
        .withConnectionConfiguration(esConfig)
        .withMaxBatchSizeBytes(options.getESMaxBatchSize())
        .withMaxBatchSize(options.getESMaxBatchSizeBytes()));

    LOG.info("Run the pipeline");
    p.run().waitUntilFinish();

  }

  /**
   * Assemble main object json with nested structure
   */
  private static String toIndex(InterpretedExtendedRecord interRecord, TemporalRecord temporal, LocationRecord location,
                                TaxonRecord taxon, MultimediaRecord multimedia, ExtendedRecord extendedRecord) {

    StringBuilder builder = new StringBuilder();
    BiConsumer<String, String> f = (k, v) -> builder.append("\"").append(k).append("\":").append(v);

    builder.append("{\"id\":\"").append(extendedRecord.getId()).append("\"").append(",");

    f.accept("raw", extendedRecord.toString());
    builder.append(",");
    f.accept("common", interRecord.toString());
    builder.append(",");
    f.accept("temporal", temporal.toString());
    builder.append(",");
    f.accept("location", location.toString());
    builder.append(",");
    f.accept("taxon", taxon.toString());
    builder.append(",");
    f.accept("multimedia", multimedia.toString());
    builder.append("}");

    return builder.toString().replaceAll("http://rs.tdwg.org/dwc/terms/", "");
  }
}