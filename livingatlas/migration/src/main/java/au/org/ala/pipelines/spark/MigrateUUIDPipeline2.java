package au.org.ala.pipelines.spark;

import static org.apache.spark.sql.functions.col;

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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple3;

/**
 * A Spark only pipeline that generates AVRO files for UUIDs based on a CSV export from Cassandra.
 *
 * <p>Previous runs should be cleaned up like so:
 *
 * <p>hdfs dfs -rm -R /pipelines-data/<globstar>/1/identifiers
 */
@Parameters(separators = "=")
public class MigrateUUIDPipeline2 implements Serializable {

  @Parameter private List<String> parameters = new ArrayList<>();

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
    MigrateUUIDPipeline2 m = new MigrateUUIDPipeline2();
    JCommander jCommander = JCommander.newBuilder().addObject(m).build();
    jCommander.parse(args);

    if (m.occFirstLoadedExportPath == null || m.targetPath == null) {
      jCommander.usage();
      System.exit(1);
    }
    m.run();
  }

  private void run() throws Exception {

    FileSystem fileSystem = getFileSystem();
    fileSystem.delete(new Path(targetPath + "/migration-tmp/csv"), true);

    System.out.println("Starting spark job to migrate UUIDs");

    System.out.println("Starting spark session");
    SparkSession spark = SparkSession.builder().appName("Migration UUIDs").getOrCreate();

    System.out.println("Load CSV");
    Dataset<Row> occFirstLoadedDataset = spark.read().csv(occFirstLoadedExportPath);

    Dataset<Tuple3<String, String, Long>> firstLoadedDataset =
        occFirstLoadedDataset
            .filter((FilterFunction<Row>) row -> StringUtils.isNotEmpty(row.getString(1)))
            .map(
                (MapFunction<Row, Tuple3<String, String, Long>>)
                    row -> {
                      return Tuple3.apply(
                          row.getString(0), // UUID
                          row.getString(1), // UUID
                          row.getString(2) == null
                                  || "firstLoaded".equals(row.getString(2)) // skip header
                              ? null
                              : LocalDateTime.parse(
                                          row.getString(2), DateTimeFormatter.ISO_DATE_TIME)
                                      .toEpochSecond(ZoneOffset.UTC)
                                  * 1000); // firstLoaded in milliseconds
                    },
                Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()));

    firstLoadedDataset
        .select(col("_1").as("datasetID"), col("_2").as("uuid"), col("_3").as("firstLoaded"))
        .write()
        .partitionBy("datasetID")
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save(targetPath + "/migration-tmp/csv");

    Path path = new Path(targetPath + "/migration-tmp/csv/");
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
        String newPath = targetPath + "/" + dataSetID + "/1/migration/";
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
