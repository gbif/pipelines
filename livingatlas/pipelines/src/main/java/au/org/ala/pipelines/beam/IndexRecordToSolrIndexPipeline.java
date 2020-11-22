package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.util.Strings;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;

@Slf4j
public class IndexRecordToSolrIndexPipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    MDC.put("step", "INDEX_RECORD_TO_SOLR");
    MDC.put("datasetId", "ALL_RECORDS");
    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "speciesLists", "index");
    ALASolrPipelineOptions options =
        PipelinesOptionsFactory.create(ALASolrPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    System.exit(0);
  }

  public static boolean hasCoordinates(IndexRecord indexRecord) {
    return indexRecord.getLatLng() != null;
  }

  public static void run(ALASolrPipelineOptions options) {

    log.info("options.getDebugCountsOnly - {}", options.getDebugCountsOnly());

    Pipeline pipeline = Pipeline.create(options);

    // join records with coordinates to sampling
    String samplingPath = options.getAllDatasetsInputPath() + "/sampling/downloads";

    // get filesystem
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    // Load Samples
    PCollection<KV<String, Map<String, String>>> sampleCollection =
        loadSamplingIntoPCollection(pipeline, samplingPath, fs);

    if (options.getDebugCountsOnly()) {
      sampleCollection
          .apply(
              MapElements.via(
                  new SimpleFunction<KV<String, Map<String, String>>, String>() {
                    @Override
                    public String apply(KV<String, Map<String, String>> input) {
                      return input.getKey();
                    }
                  }))
          .apply(
              TextIO.write()
                  .to(options.getAllDatasetsInputPath() + "/sampled-points-debug.txt")
                  .withSuffix(".txt")
                  .withoutSharding());
    }

    // Load IndexRecords
    PCollection<IndexRecord> indexRecordsCollection = loadIndexRecords(options, pipeline);

    // Filter records with coordinates - we will join these to samples
    PCollection<IndexRecord> recordsWithCoordinates =
        indexRecordsCollection.apply(Filter.by(indexRecord -> hasCoordinates(indexRecord)));

    if (options.getDebugCountsOnly()) {
      recordsWithCoordinates
          .apply(
              MapElements.via(
                  new SimpleFunction<IndexRecord, String>() {
                    @Override
                    public String apply(IndexRecord input) {
                      return input.getLatLng();
                    }
                  }))
          .apply(Distinct.create())
          .apply(
              TextIO.write()
                  .to(options.getAllDatasetsInputPath() + "/index-records-unique-points.txt")
                  .withSuffix(".txt")
                  .withoutSharding());
    }

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

    // Co group IndexRecords with coordinates with Sample data
    final TupleTag<IndexRecord> indexRecordTag = new TupleTag<>();
    final TupleTag<Map<String, String>> samplingTag = new TupleTag<>();

    // Join collections by LatLng string
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(samplingTag, sampleCollection)
            .and(indexRecordTag, recordsWithCoordinatesKeyedLatng)
            .apply(CoGroupByKey.create());

    // Create  collection which contains samples
    PCollection<IndexRecord> indexRecordsWithSampling =
        results.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, IndexRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {

                    KV<String, CoGbkResult> e = c.element();

                    Map<String, String> sampling = e.getValue().getOnly(samplingTag);
                    Iterable<IndexRecord> idIter = e.getValue().getAll(indexRecordTag);

                    idIter.forEach(
                        indexRecord -> {
                          for (Map.Entry<String, String> sample : sampling.entrySet()) {

                            if (sample.getKey().startsWith("el")) {
                              Map<String, Double> props = indexRecord.getDoubles();
                              if (props == null) {
                                props = new HashMap<String, Double>();
                                indexRecord.setDoubles(props);
                              }
                              try {
                                if (Strings.isNotBlank(sample.getValue())) {
                                  props.put(sample.getKey(), Double.parseDouble(sample.getValue()));
                                }
                              } catch (NumberFormatException ex) {
                                // do something for bad sample data
                              }
                            } else {
                              Map<String, String> props = indexRecord.getStrings();
                              if (props == null) {
                                props = new HashMap<String, String>();
                                indexRecord.setStrings(props);
                              }
                              if (Strings.isNotBlank(sample.getValue()))
                                props.put(sample.getKey(), sample.getValue());
                            }
                          }
                          c.output(indexRecord);
                        });
                  }
                }));

    if (!options.getDebugCountsOnly()) {

      // Convert to SOLR docs
      //      log.info("Adding step 4: SOLR indexing");
      //      SolrIO.ConnectionConfiguration conn =
      //          SolrIO.ConnectionConfiguration.create(options.getZkHost());

      //      // Map to SolrInputDocuments and Submit to SOLR
      //      indexRecordsWithSampling
      //          .apply(
      //              MapElements.via(
      //                  new SimpleFunction<IndexRecord, SolrInputDocument>() {
      //                    @Override
      //                    public SolrInputDocument apply(IndexRecord input) {
      //                      return ALASolrDocumentTransform.convertIndexRecordToSolrDoc(input);
      //                    }
      //                  }))
      //          .apply(
      //              SolrIO.write()
      //                  .to(options.getSolrCollection())
      //                  .withConnectionConfiguration(conn)
      //                  .withMaxBatchSize(options.getSolrBatchSize()));

      // Index records without coordinates
      //      recordsWithoutCoordinates
      //          .apply(
      //              MapElements.via(
      //                  new SimpleFunction<IndexRecord, SolrInputDocument>() {
      //                    @Override
      //                    public SolrInputDocument apply(IndexRecord input) {
      //                      return ALASolrDocumentTransform.convertIndexRecordToSolrDoc(input);
      //                    }
      //                  }))
      //          .apply(
      //              SolrIO.write()
      //                  .to(options.getSolrCollection())
      //                  .withConnectionConfiguration(conn)
      //                  .withMaxBatchSize(options.getSolrBatchSize()));
    } else {

      PCollection<Long> recordsWithoutCoordinatesCount =
          recordsWithoutCoordinates.apply(Count.globally());

      recordsWithoutCoordinatesCount
          .apply(
              MapElements.via(
                  new SimpleFunction<Long, String>() {
                    @Override
                    public String apply(Long input) {
                      return "recordsWithoutCoordinatesCount :" + input.toString();
                    }
                  }))
          .apply(
              TextIO.write()
                  .to(options.getAllDatasetsInputPath() + "/recordsWithoutCoordinatesCount.txt")
                  .withSuffix(".txt"));

      PCollection<Long> recordsWithCoordinatesCount =
          recordsWithCoordinates.apply(Count.globally());

      recordsWithCoordinatesCount
          .apply(
              MapElements.via(
                  new SimpleFunction<Long, String>() {
                    @Override
                    public String apply(Long input) {
                      return "recordsWithCoordinatesCount :" + input.toString();
                    }
                  }))
          .apply(
              TextIO.write()
                  .to(options.getAllDatasetsInputPath() + "/recordsWithCoordinatesCount.txt")
                  .withSuffix(".txt"));
    }

    pipeline.run(options).waitUntilFinish();

    log.info("Solr indexing pipeline complete");
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
    return p.apply(
        AvroIO.read(IndexRecord.class)
            .from(String.join("/", options.getAllDatasetsInputPath(), "index-record", "*/*.avro")));
  }

  private static PCollection<KV<String, Map<String, String>>> loadSamplingIntoPCollection(
      Pipeline pipeline, String samplingPath, FileSystem fs) {

    // read the headers once
    final String[] columnHeaders = getColumnHeaders(fs, samplingPath);

    // Read from the downloaded sampling CSV files
    PCollection<String> lines = pipeline.apply(TextIO.read().from(samplingPath + "/*.csv"));

    //    final CSVParser csvParser = new CSVParser();
    // Read in sampling from downloads CSV files, and key it on LatLng -> sampling
    return lines.apply(
        ParDo.of(
            new DoFn<String, KV<String, Map<String, String>>>() {
              @ProcessElement
              public void processElement(
                  @Element String sampling, OutputReceiver<KV<String, Map<String, String>>> out) {
                Map<String, String> parsedSampling = new HashMap<String, String>();
                try {
                  // skip the header
                  if (!sampling.startsWith("latitude")) {
                    // need headers as a side input
                    String[] line = sampling.split(",");
                    if (line.length == columnHeaders.length) {

                      // first two columns are latitude,longitude
                      for (int i = 2; i < columnHeaders.length; i++) {
                        if (StringUtils.trimToNull(line[i]) != null) {
                          parsedSampling.put(columnHeaders[i], line[i]);
                        }
                      }

                      String latLng = line[0] + "," + line[1];
                      KV<String, Map<String, String>> aur = KV.of(latLng, parsedSampling);
                      out.output(aur);
                    }
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e.getMessage());
                }
              }
            }));
  }

  @NotNull
  private static String[] getColumnHeaders(FileSystem fs, String samplingPath) {

    try {
      // obtain column header
      if (ALAFsUtils.exists(fs, samplingPath)) {

        Collection<String> samplingFiles = ALAFsUtils.listPaths(fs, samplingPath);

        if (!samplingFiles.isEmpty()) {

          // read the first line of the first sampling file
          String samplingFilePath = samplingFiles.iterator().next();
          String columnHeaderString =
              new BufferedReader(
                      new InputStreamReader(ALAFsUtils.openInputStream(fs, samplingFilePath)))
                  .readLine();
          return columnHeaderString.split(",");

        } else {
          throw new RuntimeException(
              "Sampling directory found, but is empty. Has sampling from spatial-service been ran ? Missing dir: "
                  + samplingPath);
        }
      } else {
        throw new RuntimeException(
            "Sampling directory cant be found. Has sampling from spatial-service been ran ? Missing dir: "
                + samplingPath);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Problem reading sampling from: " + samplingPath + " - " + e.getMessage(), e);
    }
  }
}
