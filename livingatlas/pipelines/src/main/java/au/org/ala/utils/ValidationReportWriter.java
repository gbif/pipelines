package au.org.ala.utils;

import static au.org.ala.utils.ValidationUtils.VALIDATION_REPORT_FILE;
import static java.util.Collections.reverseOrder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.MapSolrParams;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;

/**
 * Generates 2 CSV reports based on the data within system:
 *
 * <p>1) A CSV listing datasets that are in a valid state for loading 2) A CSV listing all datasets
 * and their validity for loading and test results.
 */
@Parameters(separators = "=")
@Slf4j
public class ValidationReportWriter {

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

  @Parameter(
      names = "--attempt",
      description = "Attempt of the dataset used to name the target file in file system")
  private Integer attempt;

  @Parameter(names = "--checkSolr", description = "Check SOLR", arity = 1)
  private boolean checkSolr;

  @Parameter(names = "--checkSSDS", description = "Check SDS", arity = 1)
  private boolean checkSDS = true;

  @Parameter(names = "--zkHost", description = "ZK host for SOLR")
  private String zkHost;

  @Parameter(names = "--solrCollection", description = "SOLR collection")
  private String solrCollection;

  @Parameter(names = "--timeBufferInMillis", description = "Validation time buffer in millis")
  private Integer timeBufferInMillis = 30000;

  protected static final String[] CSV_HEADERS =
      new String[] {
        "datasetID",
        "records",
        "indexed",
        "status",
        "metadata",
        "uniqueTerms",
        "dwcaToVerbatim",
        "interpretation",
        "validation",
        "uuid",
        "sds",
        "indexing",
        "emptyRecordsKeys",
        "duplicateKeys",
        ValidationUtils.EMPTY_KEY_RECORDS,
        ValidationUtils.DUPLICATE_KEY_COUNT,
        ValidationUtils.DUPLICATE_RECORD_KEY_COUNT
      };

  public static void main(String[] args) throws Exception {

    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "validation-report");

    ValidationReportWriter m = new ValidationReportWriter();
    JCommander jCommander = JCommander.newBuilder().acceptUnknownOptions(true).addObject(m).build();
    jCommander.parse(combinedArgs);

    if (m.inputPath == null || m.targetPath == null) {
      jCommander.usage();
      System.exit(1);
    }
    m.run();
  }

  public void run() throws IOException {

    FileSystem fs =
        FileSystemFactory.getInstance(HdfsConfigs.create(hdfsSiteConfig, coreSiteConfig))
            .getFs(inputPath);

    final Map<String, Long> readyToIndexCounts = new HashMap<>();

    int count;
    List<Map.Entry<String, Long>> list;
    try (FileWriter reportWriter = new FileWriter(fullReportPath)) {

      // write CSV header
      reportWriter.write(String.join(",", CSV_HEADERS) + "\n");
      count = 0;

      // retrieve indexed counts
      log.info("Checking SOLR: {}", checkSolr);
      Map<String, Long> datasetIndexCounts =
          checkSolr ? indexCounts(zkHost, solrCollection) : Collections.emptyMap();

      // iterate through directory listing
      FileStatus[] fileStatuses = fs.listStatus(new Path(inputPath));
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()) {
          String datasetID =
              fileStatus
                  .getPath()
                  .toString()
                  .substring(fileStatus.getPath().toString().lastIndexOf("/") + 1);

          // dataset found
          boolean verbatimLoaded;
          boolean uuidLoaded;
          boolean validationLoaded;
          boolean interpretationRan;
          boolean sdsRan;
          boolean indexingRan;
          boolean metadataAvailable = false;
          boolean uniqueTermsSpecified = false;
          long recordCount;
          long emptyKeyRecords = 0L;
          long duplicateKeyCount = 0L;
          long duplicateRecordKeyCount = 0L;
          count++;

          // check record count
          verbatimLoaded =
              ValidationUtils.metricsExists(
                  fs, inputPath, datasetID, attempt, ValidationUtils.VERBATIM_METRICS);
          recordCount = ValidationUtils.readVerbatimCount(fs, inputPath, datasetID, attempt);

          // check UUIDs are generated
          uuidLoaded =
              ValidationUtils.metricsExists(
                  fs, inputPath, datasetID, attempt, ValidationUtils.UUID_METRICS);
          interpretationRan =
              ValidationUtils.metricsExists(
                  fs, inputPath, datasetID, attempt, ValidationUtils.INTERPRETATION_METRICS);
          sdsRan =
              ValidationUtils.metricsExists(
                  fs, inputPath, datasetID, attempt, ValidationUtils.SENSITIVE_METRICS);
          indexingRan =
              ValidationUtils.metricsExists(
                  fs, inputPath, datasetID, attempt, ValidationUtils.INDEXING_METRICS);

          // check UUIDs are generated
          validationLoaded =
              ValidationUtils.metricsExists(
                  fs, inputPath, datasetID, attempt, VALIDATION_REPORT_FILE);

          if (validationLoaded) {

            Map<String, Object> validationMetrics =
                ValidationUtils.readValidation(fs, inputPath, datasetID, attempt);

            metadataAvailable =
                Boolean.parseBoolean(
                    validationMetrics
                        .getOrDefault(ValidationUtils.METADATA_AVAILABLE, false)
                        .toString());
            uniqueTermsSpecified =
                Boolean.parseBoolean(
                    validationMetrics
                        .getOrDefault(ValidationUtils.UNIQUE_TERMS_SPECIFIED, false)
                        .toString());
            emptyKeyRecords =
                Long.parseLong(
                    validationMetrics
                        .getOrDefault(ValidationUtils.EMPTY_KEY_RECORDS, -1L)
                        .toString());
            duplicateKeyCount =
                Long.parseLong(
                    validationMetrics
                        .getOrDefault(ValidationUtils.DUPLICATE_KEY_COUNT, -1L)
                        .toString());
            duplicateRecordKeyCount =
                Long.parseLong(
                    validationMetrics
                        .getOrDefault(ValidationUtils.DUPLICATE_RECORD_KEY_COUNT, -1L)
                        .toString());
          }

          ValidationResult validationResult =
              ValidationUtils.checkReadyForIndexing(
                  fs, inputPath, datasetID, attempt, checkSDS, timeBufferInMillis);

          // write CSV
          reportWriter.write(
              String.join(
                      ", ",
                      datasetID,
                      String.valueOf(recordCount),
                      datasetIndexCounts.getOrDefault(datasetID, -1L) >= 0
                          ? datasetIndexCounts.getOrDefault(datasetID, -1L).toString()
                          : "",
                      validationResult.getMessage(),
                      metadataAvailable ? "OK" : ValidationUtils.METADATA_NOT_AVAILABLE,
                      uniqueTermsSpecified ? "OK" : ValidationUtils.UNIQUE_TERMS_NOT_SPECIFIED,
                      verbatimLoaded ? "OK" : ValidationUtils.NO_VERBATIM,
                      interpretationRan ? "OK" : ValidationUtils.NOT_INTERPRET,
                      validationLoaded ? "OK" : ValidationUtils.NOT_VALIDATED,
                      uuidLoaded ? "OK" : ValidationUtils.UUID_REQUIRED,
                      sdsRan ? "OK" : ValidationUtils.SDS_REQUIRED,
                      indexingRan ? "OK" : ValidationUtils.NOT_INDEXED,
                      emptyKeyRecords == 0 ? "OK" : ValidationUtils.HAS_EMPTY_KEYS,
                      duplicateKeyCount == 0 ? "OK" : ValidationUtils.HAS_DUPLICATES,
                      emptyKeyRecords > 0 ? Long.toString(emptyKeyRecords) : "",
                      duplicateKeyCount > 0 ? Long.toString(duplicateKeyCount) : "",
                      duplicateRecordKeyCount > 0 ? Long.toString(duplicateRecordKeyCount) : "")
                  + "\n");
          if (recordCount > 0 && verbatimLoaded && uuidLoaded) {
            readyToIndexCounts.put(datasetID, recordCount);
          }
        }
      }

      // order by size descending
      list = new ArrayList<>(readyToIndexCounts.entrySet());
      list.sort(reverseOrder(Map.Entry.comparingByValue()));

      try (FileWriter fw = new FileWriter(targetPath)) {
        for (Map.Entry<String, Long> entry : list) {
          fw.write(entry.getKey() + "," + entry.getValue() + "\n");
        }
        fw.flush();
      }

      reportWriter.flush();
    }

    log.info("Total number of datasets: {}", count);
    log.info("Total number of valid dataset: {}", list.size());
    log.info("A list of valid datasets was written to: {}", inputPath);
    log.info("A list of all datasets and validation results was written to: {}", fullReportPath);
  }

  /** Retrieve index counts for all datasets. */
  public Map<String, Long> indexCounts(String zkHost, String solrCollection) {

    List<String> zkHosts = Arrays.stream(zkHost.split(",")).collect(Collectors.toList());

    try (SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(zkHosts, Optional.empty()).build()) {
      final Map<String, String> queryParamMap = new HashMap<>();
      queryParamMap.put("q", "*:*");
      queryParamMap.put("facet", "on");
      queryParamMap.put("rows", "0");
      queryParamMap.put("start", "0");
      queryParamMap.put("facet.field", "dataResourceUid");
      queryParamMap.put("facet.limit", "-1");
      MapSolrParams queryParams = new MapSolrParams(queryParamMap);

      QueryResponse queryResponse = cloudSolrClient.query(solrCollection, queryParams);

      FacetField ff = queryResponse.getFacetField("dataResourceUid");

      return ff.getValues().stream()
          .collect(Collectors.toMap(FacetField.Count::getName, FacetField.Count::getCount));
    } catch (Exception e) {
      log.error("Unable to retrieve counts", e);
      return Collections.emptyMap();
    }
  }
}
