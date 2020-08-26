package au.org.ala.utils;

import static java.util.Collections.reverseOrder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Generates 2 CSV reports based on the data within system:
 *
 * <p>1) A CSV listing datasets that are in a valid state for loading 2) A CSV listing all datasets
 * and their validity for loading and test results.
 */
@Parameters(separators = "=")
@Slf4j
public class DumpReadyToIndexList {

  @Parameter(
      names = "--inputPath",
      description = "The root of the pipelines data directory e.g. /data/pipelines-data ")
  private String inputPath;

  @Parameter(names = "--targetPath", description = "The file path for the generated CSV")
  private String targetPath;

  @Parameter(
      names = "--fullReportPath",
      description = "The file path for the generated CSV which is a complete list of datasets")
  private String fullReportPath;

  @Parameter(
      names = "--hdfsSiteConfig",
      description = "The absolute path to a hdfs-site.xml with default.FS configuration")
  private String hdfsSiteConfig;

  @Parameter(
      names = "--coreSiteConfig",
      description = "The absolute path to a core-site.xml with default.FS configuration")
  private String coreSiteConfig;

  public static void main(String[] args) throws Exception {

    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "dataset-validated-dump");

    DumpReadyToIndexList m = new DumpReadyToIndexList();
    JCommander jCommander = JCommander.newBuilder().acceptUnknownOptions(true).addObject(m).build();
    jCommander.parse(combinedArgs);

    if (m.inputPath == null || m.targetPath == null) {
      jCommander.usage();
      System.exit(1);
    }
    m.run();
  }

  public void run() throws Exception {

    FileSystem fs = FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(inputPath);

    Map<String, Long> readyToIndexCounts = new HashMap<String, Long>();

    FileWriter reportWriter = new FileWriter(fullReportPath);

    reportWriter.write(
        String.join(
                ", ",
                "datasetID",
                "datasetCanBeLoaded",
                "recordCount",
                ValidationUtils.METADATA_AVAILABLE,
                ValidationUtils.UNIQUE_TERMS_SPECIFIED,
                "verbatimAvoCreated",
                "validationGenerated",
                "uuidGenerated",
                "hasEmptyRecordsKeys",
                "hasDuplicateKeys",
                ValidationUtils.EMPTY_KEY_RECORDS,
                ValidationUtils.DUPLICATE_KEY_COUNT,
                ValidationUtils.DUPLICATE_RECORD_KEY_COUNT)
            + "\n");

    FileStatus[] fileStatuses = fs.listStatus(new Path(inputPath));
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetID =
            fileStatus
                .getPath()
                .toString()
                .substring(fileStatus.getPath().toString().lastIndexOf("/") + 1);

        // dataset found
        boolean verbatimLoaded = false;
        boolean uuidLoaded = false;
        boolean validationLoaded = false;
        boolean metadataAvailable = false;
        boolean uniqueTermsSpecified = false;
        Long recordCount = 0l;
        Long emptyKeyRecords = 0l;
        Long duplicateKeyCount = 0l;
        Long duplicateRecordKeyCount = 0l;

        Path metrics = new Path(fileStatus.getPath().toString() + "/1/dwca-metrics.yml");
        verbatimLoaded = fs.exists(metrics);

        if (fs.exists(metrics)) {
          // read YAML
          Yaml yaml = new Yaml();
          // the YAML files created by metrics are UTF-16 encoded
          Map<String, Object> yamlObject =
              yaml.load(new InputStreamReader(fs.open(metrics), StandardCharsets.UTF_16));

          recordCount =
              Long.parseLong(yamlObject.getOrDefault("archiveToErCountAttempted", 0L).toString());
        }

        // check UUIDs are generated
        Path uuidMetrics = new Path(fileStatus.getPath().toString() + "/1/uuid-metrics.yml");
        uuidLoaded = fs.exists(uuidMetrics);

        // check UUIDs are generated
        Path validationMetrics =
            new Path(
                fileStatus.getPath().toString() + "/1/" + ValidationUtils.VALIDATION_REPORT_FILE);
        validationLoaded = fs.exists(validationMetrics);

        if (validationLoaded) {
          // read YAML
          Yaml yaml = new Yaml();
          // the YAML files created by metrics are UTF-16 encoded
          Map<String, Object> yamlObject =
              yaml.load(new InputStreamReader(fs.open(validationMetrics), StandardCharsets.UTF_8));

          metadataAvailable =
              Boolean.parseBoolean(
                  yamlObject.getOrDefault(ValidationUtils.METADATA_AVAILABLE, false).toString());
          uniqueTermsSpecified =
              Boolean.parseBoolean(
                  yamlObject
                      .getOrDefault(ValidationUtils.UNIQUE_TERMS_SPECIFIED, false)
                      .toString());
          emptyKeyRecords =
              Long.parseLong(
                  yamlObject.getOrDefault(ValidationUtils.EMPTY_KEY_RECORDS, -1L).toString());
          duplicateKeyCount =
              Long.parseLong(
                  yamlObject.getOrDefault(ValidationUtils.DUPLICATE_KEY_COUNT, -1L).toString());
          duplicateRecordKeyCount =
              Long.parseLong(
                  yamlObject
                      .getOrDefault(ValidationUtils.DUPLICATE_RECORD_KEY_COUNT, -1L)
                      .toString());
        }

        reportWriter.write(
            String.join(
                    ", ",
                    datasetID,
                    Boolean.toString(
                        metadataAvailable
                            && uniqueTermsSpecified
                            && recordCount > 0
                            && verbatimLoaded
                            && uuidLoaded),
                    recordCount.toString(),
                    Boolean.toString(metadataAvailable),
                    Boolean.toString(uniqueTermsSpecified),
                    Boolean.toString(verbatimLoaded),
                    Boolean.toString(validationLoaded),
                    Boolean.toString(uuidLoaded),
                    Boolean.toString(emptyKeyRecords > 0),
                    Boolean.toString(duplicateKeyCount > 0),
                    emptyKeyRecords.toString(),
                    duplicateKeyCount.toString(),
                    duplicateRecordKeyCount.toString())
                + "\n");

        if (recordCount > 0 && verbatimLoaded && uuidLoaded) {
          readyToIndexCounts.put(datasetID, recordCount);
        }
      }
    }

    // order by size descending
    List<Map.Entry<String, Long>> list = new ArrayList<>(readyToIndexCounts.entrySet());
    list.sort(reverseOrder(Map.Entry.comparingByValue()));

    FileWriter fw = new FileWriter(targetPath);
    for (Map.Entry<String, Long> entry : list) {
      fw.write(entry.getKey() + "," + entry.getValue() + "\n");
    }
    fw.flush();
    fw.close();

    reportWriter.flush();
    reportWriter.close();
  }
}
