package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.SampleRecord;
import org.slf4j.MDC;

@Slf4j
public class IndexRecordToSolrIndexPipeline {

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
  }

  public static boolean hasCoordinates(IndexRecord indexRecord) {
    return indexRecord.getLatLng() != null;
  }

  public static void run(ALASolrPipelineOptions options) {

    log.info("options.getDebugCountsOnly - {}", options.getDebugCountsOnly());

    Pipeline pipeline = Pipeline.create(options);

    // get filesystem
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    // Load Samples
    PCollection<SampleRecord> sampleRecords = loadSampleRecords(options, pipeline);

    // Load IndexRecords
    PCollection<IndexRecord> indexRecordsCollection = loadIndexRecords(options, pipeline);

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

    // Create  collection which contains samples
    PCollection<IndexRecord> indexRecordsWithSampling =
        results.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, IndexRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {

                    KV<String, CoGbkResult> e = c.element();

                    SampleRecord sampleRecord =
                        e.getValue()
                            .getOnly(
                                samplingTag,
                                SampleRecord.newBuilder().setLatLng("NO_VALUE").build());
                    Iterable<IndexRecord> idIter = e.getValue().getAll(indexRecordTag);

                    if (sampleRecord.getStrings() == null && sampleRecord.getDoubles() == null) {
                      log.error("###### Sampling was empty for point: {}", e.getKey());
                    }

                    idIter.forEach(
                        indexRecord -> {
                          if (sampleRecord.getDoubles() != null) {
                            Map<String, Double> doubles = indexRecord.getDoubles();
                            if (doubles == null) {
                              doubles = new HashMap<String, Double>();
                              indexRecord.setDoubles(doubles);
                            }

                            doubles.putAll(sampleRecord.getDoubles());
                          }

                          if (sampleRecord.getStrings() != null) {
                            Map<String, String> strings = indexRecord.getStrings();
                            if (strings == null) {
                              strings = new HashMap<String, String>();
                              indexRecord.setStrings(strings);
                            }
                            strings.putAll(sampleRecord.getStrings());
                          }
                          c.output(indexRecord);
                        });
                  }
                }));

    indexRecordsWithSampling
        .apply(Count.globally())
        .apply(
            MapElements.via(
                new SimpleFunction<Long, String>() {
                  @Override
                  public String apply(Long input) {
                    return "indexRecordsWithSampling :" + input.toString();
                  }
                }))
        .apply(
            TextIO.write()
                .to(
                    options.getAllDatasetsInputPath()
                        + "/index-record-debug/indexRecordsWithSampling")
                .withSuffix(".txt"));

    recordsWithoutCoordinates
        .apply(Count.globally())
        .apply(
            MapElements.via(
                new SimpleFunction<Long, String>() {
                  @Override
                  public String apply(Long input) {
                    return "indexRecordsWithoutSampling :" + input.toString();
                  }
                }))
        .apply(
            TextIO.write()
                .to(
                    options.getAllDatasetsInputPath()
                        + "/index-record-debug/indexRecordsWithoutSampling")
                .withSuffix(".txt"));

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

  private static PCollection<SampleRecord> loadSampleRecords(
      ALASolrPipelineOptions options, Pipeline p) {
    return p.apply(
        AvroIO.read(SampleRecord.class)
            .from(String.join("/", options.getAllDatasetsInputPath(), "sampling", "*.avro")));

    //    // read the headers once
    //    final String[] columnHeaders = getColumnHeaders(fs, samplingPath);
    //
    //    // Read from the downloaded sampling CSV files
    //    PCollection<String> lines = pipeline.apply(TextIO.read().from(samplingPath + "/*.csv"));
    //
    //    // Read in sampling from downloads CSV files, and key it on LatLng -> sampling
    //    return lines.apply(
    //        ParDo.of(
    //            new DoFn<String, KV<String, Map<String, String>>>() {
    //
    //              @ProcessElement
    //              public void processElement(
    //                  @Element String sampling, OutputReceiver<KV<String, Map<String, String>>>
    // out) {
    //                Map<String, String> parsedSampling = new HashMap<String, String>();
    //                try {
    //                  // skip the header
    //                  if (!sampling.startsWith("latitude")) {
    //                    // need headers as a side input
    //                    final CSVParser csvParser = new CSVParser();
    //
    //                    String[] line = csvParser.parseLine(sampling);
    //                    if (line.length == columnHeaders.length) {
    //
    //                      // first two columns are latitude,longitude
    //                      for (int i = 2; i < columnHeaders.length; i++) {
    //                        if (StringUtils.trimToNull(line[i]) != null) {
    //                          parsedSampling.put(columnHeaders[i], line[i]);
    //                        }
    //                      }
    //
    //                      String latLng = line[0] + "," + line[1];
    //                      KV<String, Map<String, String>> aur = KV.of(latLng, parsedSampling);
    //                      out.output(aur);
    //                    }
    //                  }
    //                } catch (Exception e) {
    //                  throw new RuntimeException(e.getMessage());
    //                }
    //              }
    //            }));
  }

  //  @NotNull
  //  private static String[] getColumnHeaders(FileSystem fs, String samplingPath) {
  //
  //    try {
  //      // obtain column header
  //      if (ALAFsUtils.exists(fs, samplingPath)) {
  //
  //        Collection<String> samplingFiles = ALAFsUtils.listPaths(fs, samplingPath);
  //
  //        if (!samplingFiles.isEmpty()) {
  //
  //          // read the first line of the first sampling file
  //          String samplingFilePath = samplingFiles.iterator().next();
  //          String columnHeaderString =
  //              new BufferedReader(
  //                      new InputStreamReader(ALAFsUtils.openInputStream(fs, samplingFilePath)))
  //                  .readLine();
  //          return columnHeaderString.split(",");
  //
  //        } else {
  //          throw new RuntimeException(
  //              "Sampling directory found, but is empty. Has sampling from spatial-service been
  // ran ? Missing dir: "
  //                  + samplingPath);
  //        }
  //      } else {
  //        throw new RuntimeException(
  //            "Sampling directory cant be found. Has sampling from spatial-service been ran ?
  // Missing dir: "
  //                + samplingPath);
  //      }
  //    } catch (IOException e) {
  //      throw new RuntimeException(
  //          "Problem reading sampling from: " + samplingPath + " - " + e.getMessage(), e);
  //    }
  //  }
}
