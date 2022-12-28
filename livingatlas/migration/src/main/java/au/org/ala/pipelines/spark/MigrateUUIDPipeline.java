package au.org.ala.pipelines.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple4;

/**
 * A Spark only pipeline that generates AVRO files for UUIDs based on a CSV export from Cassandra.
 *
 * <p>Previous runs should be cleaned up like so:
 *
 * <p>hdfs dfs -rm -R /pipelines-data/<globstar>/1/identifiers
 */
@Parameters(separators = "=")
public class MigrateUUIDPipeline implements Serializable {

  @Parameter private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--occUuidExportPath",
      description =
          "The input path to a CSV export from occ_uuid in cassandra e.g /data/occ_uuid.csv or hdfs://localhost:8020/occ_uuid.csv")
  private String occUuidExportPath;

  @Parameter(
      names = "--occFirstLoadedExportPath",
      description =
          "The input path to a CSV export from occ_uuid in cassandra e.g /data/occ_uuid.csv or hdfs://localhost:8020/occ_uuid.csv")
  private String occFirstLoadedExportPath;

  @Parameter(
      names = "--targetPath",
      description = "The output path e.g /data or hdfs://localhost:8020")
  private String targetPath;

  @Parameter(
      names = "--coreSiteConfig",
      description = "The absolute path to a hdfs-site.xml with default.FS configuration")
  private String coreSiteConfig;

  @Parameter(
      names = "--hdfsSiteConfig",
      description = "The absolute path to a hdfs-site.xml with default.FS configuration")
  private String hdfsSiteConfig;

  public static void main(String[] args) throws Exception {
    MigrateUUIDPipeline m = new MigrateUUIDPipeline();
    JCommander jCommander = JCommander.newBuilder().addObject(m).build();
    jCommander.parse(args);

    if (m.occUuidExportPath == null || m.targetPath == null) {
      jCommander.usage();
      System.exit(1);
    }
    m.run();
  }

  private void run() throws Exception {

    FileSystem fileSystem = getFileSystem();
    fileSystem.delete(new Path(targetPath + "/migration-tmp/avro"), true);

    System.out.println("Starting spark job to migrate UUIDs");
    Schema schemaAvro =
        new Schema.Parser()
            .parse(
                MigrateUUIDPipeline.class
                    .getClassLoader()
                    .getResourceAsStream("ala-uuid-record.avsc"));

    System.out.println("Starting spark session");
    SparkSession spark = SparkSession.builder().appName("Migration UUIDs").getOrCreate();

    System.out.println("Load CSV");
    Dataset<Row> occUuidDataset = spark.read().csv(occUuidExportPath);

    System.out.println("Load CSV");
    Dataset<Row> occFirstLoadedDataset = spark.read().csv(occFirstLoadedExportPath);

    System.out.println("Load UUIDs");
    Dataset<Tuple4<String, String, String, String>> uuidRecords =
        occUuidDataset
            .filter(
                (FilterFunction<Row>)
                    row ->
                        StringUtils.isNotEmpty(row.getString(0))
                            && row.getString(0).contains("|")
                            && row.getString(0).startsWith("dr"))
            .map(
                (MapFunction<Row, Tuple4<String, String, String, String>>)
                    row -> {
                      String datasetID =
                          row.getString(0).substring(0, row.getString(0).indexOf("|"));
                      return Tuple4.apply(
                          datasetID, // datasetID
                          "temp_" + datasetID + "_" + row.getString(1),
                          row.getString(1), // UUID
                          row.getString(0)); // UniqueKey - constructed from DwC (and non-DwC terms)
                    },
                Encoders.tuple(
                    Encoders.STRING(), Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

    Dataset<Tuple2<String, Long>> firstLoadedDataset =
        occFirstLoadedDataset
            .filter((FilterFunction<Row>) row -> StringUtils.isNotEmpty(row.getString(1)))
            .map(
                (MapFunction<Row, Tuple2<String, Long>>)
                    row -> {
                      return Tuple2.apply(
                          row.getString(0), // UUID
                          row.getString(1) == null
                                  || "firstLoaded".equals(row.getString(1)) // skip header
                              ? null
                              : LocalDateTime.parse(
                                      row.getString(1), DateTimeFormatter.ISO_DATE_TIME)
                                  .toEpochSecond(ZoneOffset.UTC)); // firstLoaded
                    },
                Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

    Dataset<Row> combined =
        uuidRecords.join(
            firstLoadedDataset, uuidRecords.col("_3").equalTo(firstLoadedDataset.col("_1")));

    combined
        .select(
            uuidRecords.col("_1").as("datasetID"),
            uuidRecords.col("_2").as("id"),
            uuidRecords.col("_3").as("uuid"),
            uuidRecords.col("_4").as("uniqueKey"),
            firstLoadedDataset.col("_2").as("firstLoaded"))
        .write()
        .partitionBy("datasetID")
        .format("avro")
        .option("avroSchema", schemaAvro.toString())
        .mode(SaveMode.Overwrite)
        .save(targetPath + "/migration-tmp/avro");

    Path path = new Path(targetPath + "/migration-tmp/avro/");
    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
    while (iterator.hasNext()) {

      // datasetID=dr1
      LocatedFileStatus locatedFileStatus = iterator.next();
      Path sourcePath = locatedFileStatus.getPath();
      String fullPath = sourcePath.toString();

      if (fullPath.lastIndexOf("=") > 0) {
        String dataSetID =
            fullPath.substring(fullPath.lastIndexOf("=") + 1, fullPath.lastIndexOf("/"));

        // move to correct location
        String newPath = targetPath + "/" + dataSetID + "/1/identifiers/ala_uuid/";
        fileSystem.mkdirs(new Path(newPath));

        Path destination = new Path(newPath + sourcePath.getName());
        fileSystem.rename(sourcePath, destination);
      }
    }

    System.out.println("Remove temp directories");
    fileSystem.delete(new Path(targetPath + "/migration-tmp"), true);

    System.out.println("Close session");
    spark.close();
    System.out.println("Closed session. Job finished.");
  }

  private FileSystem getFileSystem() throws IOException {
    // move to correct directory structure
    // check if the hdfs-site.xml is provided
    Configuration configuration = new Configuration();
    System.out.println("Using FS: " + hdfsSiteConfig + " and " + coreSiteConfig);
    if (!Strings.isNullOrEmpty(hdfsSiteConfig) && !Strings.isNullOrEmpty(coreSiteConfig)) {
      File hdfsSite = new File(hdfsSiteConfig);
      if (hdfsSite.exists() && hdfsSite.isFile()) {
        System.out.println("Setting up HDFS with: " + hdfsSiteConfig);
        configuration.addResource(hdfsSite.toURI().toURL());
      }
      File coreSite = new File(coreSiteConfig);
      if (coreSite.exists() && coreSite.isFile()) {
        System.out.println("Setting up HDFS with: " + coreSiteConfig);
        configuration.addResource(coreSite.toURI().toURL());
      }
    }

    // get a list of paths & move to correct directories
    return FileSystem.get(configuration);
  }
}
