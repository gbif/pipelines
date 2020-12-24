package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.transforms.IndexRecordTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import avro.shaded.com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.JackKnifeOutlierRecord;
import org.gbif.pipelines.io.avro.SampleRecord;
import org.joda.time.Duration;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

/** Pipeline that joins sample data and index records and indexes to SOLR. */
@Slf4j
public class IndexRecordToSolrPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    MDC.put("step", "INDEX_RECORD_TO_SOLR");

    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "speciesLists", "index");
    ALASolrPipelineOptions options =
        PipelinesOptionsFactory.create(ALASolrPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId() != null ? options.getDatasetId() : "ALL_RECORDS");
    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static boolean hasCoordinates(IndexRecord indexRecord) {
    return indexRecord.getLatLng() != null;
  }

  public static void run(ALASolrPipelineOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    // Load IndexRecords
    PCollection<IndexRecord> indexRecordsCollection = loadIndexRecords(options, pipeline);

    if (options.getIncludeSampling()) {

      // Load Samples
      PCollection<SampleRecord> sampleRecords = loadSampleRecords(options, pipeline);

      // Filter records with coordinates - we will join these to samples
      PCollection<IndexRecord> recordsWithCoordinates =
          indexRecordsCollection.apply(Filter.by(indexRecord -> hasCoordinates(indexRecord)));

      // Filter records without coordinates - we will index, but not sample these
      PCollection<IndexRecord> recordsWithoutCoordinates =
          indexRecordsCollection.apply(Filter.by(indexRecord -> !hasCoordinates(indexRecord)));

      // Convert to KV <LatLng, IndexRecord>
      PCollection<KV<String, IndexRecord>> recordsWithCoordinatesKeyedLatng =
          recordsWithCoordinates.apply(
              MapElements.via(
                  new SimpleFunction<IndexRecord, KV<String, IndexRecord>>() {
                    @Override
                    public KV<String, IndexRecord> apply(IndexRecord input) {
                      return KV.of(input.getLatLng(), input);
                    }
                  }));

      // Convert to KV <LatLng, SampleRecord>
      PCollection<KV<String, SampleRecord>> sampleRecordsKeyedLatng =
          sampleRecords.apply(
              MapElements.via(
                  new SimpleFunction<SampleRecord, KV<String, SampleRecord>>() {
                    @Override
                    public KV<String, SampleRecord> apply(SampleRecord input) {
                      return KV.of(input.getLatLng(), input);
                    }
                  }));

      // Co group IndexRecords with coordinates with Sample data
      final TupleTag<IndexRecord> indexRecordTag = new TupleTag<>();
      final TupleTag<SampleRecord> samplingTag = new TupleTag<>();

      // Join collections by LatLng string
      PCollection<KV<String, CoGbkResult>> results =
          KeyedPCollectionTuple.of(samplingTag, sampleRecordsKeyedLatng)
              .and(indexRecordTag, recordsWithCoordinatesKeyedLatng)
              .apply(CoGroupByKey.create());

      // Create collection which contains samples keyed with indexRecord.id
      PCollection<KV<String, IndexRecord>> indexRecordsWithSampling =
          results.apply(ParDo.of(joinSampling(indexRecordTag, samplingTag)));

      // Load Jackknife
      PCollection<JackKnifeOutlierRecord> jackKnifeRecords =
          loadJackKnifeRecords(options, pipeline);

      // Convert to KV <recordID, JackKnifeOutlierRecord>
      PCollection<KV<String, JackKnifeOutlierRecord>> jackKnifeRecordsKeyedRecordID =
          jackKnifeRecords.apply(
              MapElements.via(
                  new SimpleFunction<JackKnifeOutlierRecord, KV<String, JackKnifeOutlierRecord>>() {
                    @Override
                    public KV<String, JackKnifeOutlierRecord> apply(JackKnifeOutlierRecord input) {
                      return KV.of(input.getId(), input);
                    }
                  }));

      final JackKnifeOutlierRecord nullJkor =
          JackKnifeOutlierRecord.newBuilder().setId("").setItems(new ArrayList<>()).build();

      // Join indexRecordsSampling and jackKnife
      PCollection<KV<String, KV<IndexRecord, JackKnifeOutlierRecord>>>
          indexRecordSamplingJoinJackKnife =
              Join.leftOuterJoin(indexRecordsWithSampling, jackKnifeRecordsKeyedRecordID, nullJkor);

      // Create collection which contains samples and is keyed by recordID
      PCollection<IndexRecord> indexRecordWithSamplingAndJackKnife =
          indexRecordSamplingJoinJackKnife.apply(
              ParDo.of(
                  new DoFn<KV<String, KV<IndexRecord, JackKnifeOutlierRecord>>, IndexRecord>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {

                      KV<String, KV<IndexRecord, JackKnifeOutlierRecord>> e = c.element();
                      IndexRecord indexRecord = e.getValue().getKey();
                      JackKnifeOutlierRecord jkor = e.getValue().getValue();

                      if (jkor != nullJkor) {
                        Map<String, Integer> ints = indexRecord.getInts();
                        if (ints == null) {
                          ints = new HashMap<String, Integer>();
                          indexRecord.setInts(ints);
                        }
                        ints.put("outlierLayerCount", jkor.getItems().size());

                        Map<String, List<String>> stringsList = indexRecord.getMultiValues();
                        if (stringsList == null) {
                          stringsList = new HashMap();
                          indexRecord.setMultiValues(stringsList);
                        }

                        stringsList.put("outlierLayer", jkor.getItems());
                      }

                      c.output(indexRecord);
                    }
                  }));

      log.info("Adding step 4: SOLR indexing");
      SolrIO.ConnectionConfiguration conn =
          SolrIO.ConnectionConfiguration.create(options.getZkHost());

      indexRecordWithSamplingAndJackKnife
          .apply(
              "SOLR_indexRecordsWithSampling",
              ParDo.of(new IndexRecordTransform.IndexRecordToSolrInputDocumentFcn()))
          .apply(
              SolrIO.write()
                  .to(options.getSolrCollection())
                  .withConnectionConfiguration(conn)
                  .withMaxBatchSize(options.getSolrBatchSize())
                  .withRetryConfiguration(
                      SolrIO.RetryConfiguration.create(
                          options.getSolrRetryMaxAttempts(),
                          Duration.standardMinutes(options.getSolrRetryDurationInMins()))));

      recordsWithoutCoordinates
          .apply(
              "SOLR_recordsWithoutCoordinates",
              ParDo.of(new IndexRecordTransform.IndexRecordToSolrInputDocumentFcn()))
          .apply(
              SolrIO.write()
                  .to(options.getSolrCollection())
                  .withConnectionConfiguration(conn)
                  .withMaxBatchSize(options.getSolrBatchSize())
                  .withRetryConfiguration(
                      SolrIO.RetryConfiguration.create(
                          options.getSolrRetryMaxAttempts(),
                          Duration.standardMinutes(options.getSolrRetryDurationInMins()))));
    } else {

      log.info("Adding step 4: SOLR indexing");
      SolrIO.ConnectionConfiguration conn =
          SolrIO.ConnectionConfiguration.create(options.getZkHost());

      indexRecordsCollection
          .apply(
              "SOLR_indexRecords",
              ParDo.of(new IndexRecordTransform.IndexRecordToSolrInputDocumentFcn()))
          .apply(
              SolrIO.write()
                  .to(options.getSolrCollection())
                  .withConnectionConfiguration(conn)
                  .withMaxBatchSize(options.getSolrBatchSize())
                  .withRetryConfiguration(
                      SolrIO.RetryConfiguration.create(
                          options.getSolrRetryMaxAttempts(),
                          Duration.standardMinutes(options.getSolrRetryDurationInMins()))));
    }

    pipeline.run(options).waitUntilFinish();

    log.info("Solr indexing pipeline complete");
  }

  private static DoFn<KV<String, CoGbkResult>, KV<String, IndexRecord>> joinSampling(
      TupleTag<IndexRecord> indexRecordTag, TupleTag<SampleRecord> samplingTag) {

    return new DoFn<KV<String, CoGbkResult>, KV<String, IndexRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {

        KV<String, CoGbkResult> e = c.element();

        SampleRecord sampleRecord =
            e.getValue()
                .getOnly(samplingTag, SampleRecord.newBuilder().setLatLng("NO_VALUE").build());
        Iterable<IndexRecord> indexRecordIterable = e.getValue().getAll(indexRecordTag);

        if (sampleRecord.getStrings() == null && sampleRecord.getDoubles() == null) {
          log.error("Sampling was empty for point: {}", e.getKey());
        }

        indexRecordIterable.forEach(
            indexRecord -> {
              Map<String, String> strings =
                  indexRecord.getStrings() != null
                      ? indexRecord.getStrings()
                      : new HashMap<String, String>();
              Map<String, Double> doubles =
                  indexRecord.getDoubles() != null
                      ? indexRecord.getDoubles()
                      : new HashMap<String, Double>();

              Map<String, String> stringsToPersist =
                  ImmutableMap.<String, String>builder()
                      .putAll(strings)
                      .putAll(sampleRecord.getStrings())
                      .build();

              Map<String, Double> doublesToPersist =
                  ImmutableMap.<String, Double>builder()
                      .putAll(doubles)
                      .putAll(sampleRecord.getDoubles())
                      .build();

              IndexRecord ir =
                  IndexRecord.newBuilder()
                      .setId(indexRecord.getId())
                      .setTaxonID(indexRecord.getTaxonID())
                      .setLatLng(indexRecord.getLatLng())
                      .setMultiValues(indexRecord.getMultiValues())
                      .setDates(indexRecord.getDates())
                      .setLongs(indexRecord.getLongs())
                      .setBooleans(indexRecord.getBooleans())
                      .setInts(indexRecord.getInts())
                      .setStrings(stringsToPersist)
                      .setDoubles(doublesToPersist)
                      .build();

              c.output(KV.of(indexRecord.getId(), ir));
            });
      }
    };
  }

  /**
   * Load index records from AVRO.
   *
   * @param options
   * @param p
   * @return
   */
  private static PCollection<IndexRecord> loadIndexRecords(
      ALASolrPipelineOptions options, Pipeline p) {
    if (options.getDatasetId() != null && !"all".equalsIgnoreCase(options.getDatasetId())) {
      return p.apply(
          AvroIO.read(IndexRecord.class)
              .from(
                  String.join(
                      "/",
                      options.getAllDatasetsInputPath(),
                      "index-record",
                      options.getDatasetId() + "/*.avro")));
    }
    return p.apply(
        AvroIO.read(IndexRecord.class)
            .from(String.join("/", options.getAllDatasetsInputPath(), "index-record", "*/*.avro")));
  }

  private static PCollection<SampleRecord> loadSampleRecords(
      ALASolrPipelineOptions options, Pipeline p) {
    String samplingPath =
        String.join("/", ALAFsUtils.buildPathSamplingUsingTargetPath(options), "*.avro");
    log.info("Loading sampling from {}", samplingPath);
    return p.apply(AvroIO.read(SampleRecord.class).from(samplingPath));
  }

  private static PCollection<JackKnifeOutlierRecord> loadJackKnifeRecords(
      ALASolrPipelineOptions options, Pipeline p) {
    String jackknifePath =
        PathBuilder.buildPath(options.getJackKnifePath(), "outliers", "*" + AVRO_EXTENSION)
            .toString();
    log.info("Loading jackknife from {}", jackknifePath);
    return p.apply(AvroIO.read(JackKnifeOutlierRecord.class).from(jackknifePath));
  }
}
