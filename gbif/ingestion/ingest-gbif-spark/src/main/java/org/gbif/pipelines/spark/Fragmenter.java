package org.gbif.pipelines.spark;

import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.coordinator.DistributedUtil.timeAndRecPerSecond;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.Keygen;
import org.gbif.pipelines.keygen.OccurrenceRecord;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;
import org.gbif.pipelines.transform.utils.KeygenServiceFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;
import scala.Tuple2;

@Slf4j
public class Fragmenter {

  public static final String METRICS_FILENAME = "fragmenter.yml";
  private static final TermFactory TERM_FACTORY = TermFactory.instance();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = "--tripletValid",
        description = "DWCA validation from crawler, all triplets are unique",
        required = false,
        arity = 1)
    private boolean tripletValid = false;

    @Parameter(
        names = "--occurrenceIdValid",
        description = "DWCA validation from crawler, all occurrenceIds are unique",
        required = false,
        arity = 1)
    private boolean occurrenceIdValid = true;

    @Parameter(
        names = "--config",
        description = "Path to YAML configuration file",
        required = false)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    boolean help;
  }

  public static void main(String[] argsv) throws Exception {

    Fragmenter.Args args = new Fragmenter.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true); // FIXME to ease airflow/registry integration
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    // get config, spark session and filesystem initialised
    PipelinesConfig config = loadConfig(args.config);
    SparkSession spark =
        getSparkSession(args.master, args.appName, config, Fragmenter::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    // run main pipeline
    runFragmenter(
        spark,
        fileSystem,
        config,
        args.datasetId,
        args.attempt,
        args.tripletValid,
        args.occurrenceIdValid);

    // shutdown
    spark.stop();
    spark.close();
    fileSystem.close();
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    // nothing
  }

  public static void runFragmenter(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      Integer attempt,
      boolean useTriplet,
      boolean useOccurrenceId)
      throws Exception {

    MDC.put("datasetKey", datasetId);
    long start = System.currentTimeMillis();
    log.info("Starting to run fragmenter for dataset {}, attempt {}", datasetId, attempt);

    String outputPath = config.getOutputPath() + "/" + datasetId + "/" + attempt;

    // Configure HFile output
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.set(FileOutputFormat.COMPRESS, "true");
    hbaseConf.setClass(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
    hbaseConf.addResource(new Path("/etc/hadoop/conf/hbase-site.xml"));
    hbaseConf.set("hbase.fs.tmp.dir", outputPath + "/hfile-staging");
    hbaseConf.set("hbase.mapreduce.hfileoutputformat.table.name", config.getFragmentsTable());

    if (config.getHdfsSiteConfig() != null && config.getCoreSiteConfig() != null) {
      hbaseConf.addResource(new Path(config.getHdfsSiteConfig()));
      hbaseConf.addResource(new Path(config.getCoreSiteConfig()));
    }

    try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(config.getFragmentsTable()));
        RegionLocator regionLocator =
            connection.getRegionLocator(TableName.valueOf(config.getFragmentsTable()))) {

      // read verbatim records
      Dataset<ExtendedRecord> verbatim =
          spark
              .read()
              .format("parquet")
              .load(outputPath + "/" + Directories.OCCURRENCE_VERBATIM)
              .as(Encoders.bean(ExtendedRecord.class));

      // convert to dwca
      SerializableSupplier<HBaseLockingKey> supplier =
          new SerializableSupplier<HBaseLockingKey>() {
            @Override
            public HBaseLockingKey get() {
              return KeygenServiceFactory.create(config, datasetId);
            }
          };

      Dataset<org.apache.spark.sql.Row> rawRecords =
          convertToRawRecords(verbatim, supplier, useTriplet, useOccurrenceId)
              .toDF()
              .orderBy("key");

      long recordCount = rawRecords.count();
      log.debug("Count: {}", rawRecords.count());

      // write hfiles
      JavaPairRDD<Tuple2<String, String>, String> hbaseKvs =
          rawRecords
              .javaRDD()
              .flatMapToPair(
                  record -> {
                    List<Tuple2<Tuple2<String, String>, String>> cells = new ArrayList<>();
                    cells.add(
                        new Tuple2<>(
                            new Tuple2<>(record.getAs("key"), "attempt"), String.valueOf(attempt)));
                    cells.add(
                        new Tuple2<>(
                            new Tuple2<>(record.getAs("key"), "dateCreated"),
                            ((Long) record.getAs("createdDate")).toString()));
                    cells.add(
                        new Tuple2<>(
                            new Tuple2<>(record.getAs("key"), "protocol"),
                            EndpointType.DWC_ARCHIVE.name()));
                    cells.add(
                        new Tuple2<>(
                            new Tuple2<>(record.getAs("key"), "record"),
                            record.getAs("recordBody")));
                    return cells.iterator();
                  });

      cleanHdfsPath(fileSystem, config, outputPath);
      String hfilePath = outputPath + "/fragment";

      Job job = Job.getInstance(hbaseConf);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(org.apache.hadoop.hbase.KeyValue.class);
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

      hbaseKvs
          .mapToPair(
              cell -> {
                ImmutableBytesWritable k = new ImmutableBytesWritable(Bytes.toBytes(cell._1._1));
                Cell row =
                    new KeyValue(
                        Bytes.toBytes(cell._1._1), // key
                        Bytes.toBytes("fragment"), // column family
                        Bytes.toBytes(cell._1._2), // cell
                        Bytes.toBytes(cell._2) // cell value
                        );
                return new Tuple2<>(k, row);
              })
          .saveAsNewAPIHadoopFile(
              hfilePath,
              ImmutableBytesWritable.class,
              KeyValue.class,
              HFileOutputFormat2.class,
              hbaseConf);

      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
      loader.doBulkLoad(new Path(hfilePath), admin, table, regionLocator);

      writeMetricsYaml(
          fileSystem,
          Map.of("fragmenterRecordsCountAttempted", recordCount),
          outputPath + "/" + METRICS_FILENAME);

      log.info(timeAndRecPerSecond("fragmenter", start, recordCount));
    } catch (Exception e) {
      log.error("Error during fragmenter: {}", e.getMessage(), e);
      throw e;
    } finally {
      MDC.clear();
    }
  }

  @NotNull
  private static void cleanHdfsPath(
      FileSystem fileSystem, PipelinesConfig config, String outputPath) throws IOException {
    Path fragmentPath = new Path(outputPath + "/fragment");
    if (fileSystem.exists(fragmentPath)) {
      fileSystem.delete(fragmentPath, true);
    }
    Path hfilesPath = new Path(outputPath + "/hfile-staging");
    if (fileSystem.exists(hfilesPath)) {
      fileSystem.delete(hfilesPath, true);
    }
  }

  private static Dataset<RawRecord> convertToRawRecords(
      Dataset<ExtendedRecord> verbatim,
      SerializableSupplier<HBaseLockingKey> keygenService,
      boolean useTriplet,
      boolean useOccurrenceId) {
    return verbatim
        .map(
            (MapFunction<ExtendedRecord, RawRecord>)
                extendedRecord -> {
                  String stringRecord = getStringRecord(extendedRecord);
                  String tripletId = getTriplet(extendedRecord);
                  String occurrenceId = getOccurrenceId(extendedRecord);
                  DwcOccurrenceRecord dor =
                      DwcOccurrenceRecord.builder()
                          .occurrenceId(occurrenceId)
                          .triplet(tripletId)
                          .stringRecord(stringRecord)
                          .build();
                  Predicate<String> emptyValidator = s -> true;
                  return convertToRawRecord(
                      keygenService, emptyValidator, useTriplet, useOccurrenceId, dor);
                },
            Encoders.bean(RawRecord.class))
        .filter((FilterFunction<RawRecord>) Objects::nonNull);
  }

  public static RawRecord convertToRawRecord(
      SerializableSupplier<HBaseLockingKey> keygenService,
      Predicate<String> validator,
      boolean useTriplet,
      boolean useOccurrenceId,
      DwcOccurrenceRecord or) {

    Optional<Long> key = Optional.of(Keygen.getErrorKey());
    try {
      key = Keygen.getKey(keygenService.get(), useTriplet, useOccurrenceId, false, or);
    } catch (RuntimeException ex) {
      log.error(ex.getMessage(), ex);
    }

    if (key.isEmpty()
        || Keygen.getErrorKey().equals(key.get())
        || !validator.test(key.toString())) {
      return null;
    }

    return RawRecord.builder()
        .key(Keygen.getSaltedKey(key.get()))
        .recordBody(or.getStringRecord())
        .createdDate(System.currentTimeMillis()) // FIXME - placeholder
        .build();
  }

  private static String getTriplet(ExtendedRecord er) {
    String ic = extractNullAwareValue(er, DwcTerm.institutionCode);
    String cc = extractNullAwareValue(er, DwcTerm.collectionCode);
    String cn = extractNullAwareValue(er, DwcTerm.catalogNumber);
    return OccurrenceKeyBuilder.buildKey(ic, cc, cn).orElse(null);
  }

  private static String getOccurrenceId(ExtendedRecord er) {
    return extractNullAwareValue(er, DwcTerm.occurrenceID);
  }

  private static String getStringRecord(ExtendedRecord er) {
    // we need alphabetically sorted maps to guarantee that identical records have identical JSON
    Map<String, Object> data = new TreeMap<>();

    data.put("id", er.getId());

    // Put in all core terms
    er.getCoreTerms()
        .forEach(
            (t, value) -> {
              String ct = TERM_FACTORY.findTerm(t).simpleName();
              data.put(ct, value);
            });

    if (!er.getExtensions().isEmpty()) {
      Map<Term, List<Map<String, String>>> extensions =
          new TreeMap<>(Comparator.comparing(Term::qualifiedName));
      data.put("extensions", extensions);

      // iterate over extensions
      er.getExtensions()
          .forEach(
              (ex, values) -> {
                List<Map<String, String>> records =
                    new ArrayList<>(er.getExtensions().get(ex).size());

                // iterate over extensions records
                values.forEach(
                    m -> {
                      Map<String, String> edata = new TreeMap<>();

                      m.forEach(
                          (t, value) -> {
                            String evt = TERM_FACTORY.findTerm(t).simpleName();
                            edata.put(evt, value);
                          });
                      records.add(edata);
                    });

                Term et = TERM_FACTORY.findTerm(ex);
                extensions.put(et, records);
              });
    }
    // serialize to json
    try {
      return MAPPER.writeValueAsString(data);
    } catch (IOException e) {
      log.error("Cannot serialize star record data", e);
    }
    return "";
  }

  @Data
  @Builder
  public static class DwcOccurrenceRecord implements OccurrenceRecord {
    private String triplet;
    private String occurrenceId;
    private String stringRecord;

    @Override
    public String getStringRecord() {
      return stringRecord;
    }

    @Override
    public Optional<String> getOccurrenceId() {
      return Optional.ofNullable(occurrenceId);
    }

    @Override
    public Optional<String> getTriplet() {
      return Optional.ofNullable(triplet);
    }
  }
}
