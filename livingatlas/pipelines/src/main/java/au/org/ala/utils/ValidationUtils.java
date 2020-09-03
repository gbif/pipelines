package au.org.ala.utils;

import static au.org.ala.pipelines.beam.ALAUUIDMintingPipeline.UNIQUE_COMPOSITE_KEY_JOIN_CHAR;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

@Slf4j
public class ValidationUtils {

  public static final String CANT_INDEX = "CANT_INDEX";
  public static final String METADATA_NOT_AVAILABLE = "NOT_AVAILABLE";
  public static final String UNIQUE_TERMS_NOT_SPECIFIED = "NOT_SPECIFIED";
  public static final String NO_VERBATIM = "NO_VERBATIM";
  public static final String NOT_INTERPRET = "NOT_INTERPRET";
  public static final String NOT_VALIDATED = "NOT_VALIDATED";
  public static final String UUID_REQUIRED = "UUID_REQUIRED";
  public static final String NOT_SAMPLED = "NOT_SAMPLED";
  public static final String NOT_INDEXED = "NOT_INDEXED";
  public static final String HAS_EMPTY_KEYS = "HAS_EMPTY_KEYS";
  public static final String HAS_DUPLICATES = "HAS_DUPLICATES";
  public static final String RESAMPLING_REQUIRED = "RESAMPLING_REQUIRED";

  public static final String UUID_METRICS = "uuid-metrics.yml";
  public static final String INTERPRETATION_METRICS = "interpretation-metrics.yml";
  public static final String VERBATIM_METRICS = "dwca-metrics.yml";
  public static final String SAMPLING_METRICS = "sampling-metrics.yml";
  public static final String INDEXING_METRICS = "indexing-metrics.yml";

  public static final String DUPLICATE_KEY_COUNT = "duplicateKeyCount";
  public static final String EMPTY_KEY_RECORDS = "emptyKeyRecords";
  public static final String DUPLICATE_RECORD_KEY_COUNT = "duplicateRecordKeyCount";
  public static final String VALIDATION_REPORT_FILE = "validation-report.yaml";
  public static final String VALIDATION_OUTPUT_DIR = "validation";
  public static final String DUPLICATE_KEYS_OUTPUT = "duplicateKeys.csv";
  public static final String UNIQUE_TERMS_SPECIFIED = "uniqueTermsSpecified";
  public static final String METADATA_AVAILABLE = "metadataAvailable";

  /**
   * Checks a dataset can be indexed.
   *
   * @param options
   * @return
   */
  public static ValidationResult checkReadyForIndexing(ALASolrPipelineOptions options)
      throws Exception {

    ValidationResult isValid = checkValidationFile(options);
    if (!isValid.getValid()) {
      return isValid;
    }

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());

    return checkReadyForIndexing(
        fs,
        options.getInputPath(),
        options.getDatasetId(),
        options.getAttempt(),
        options.getIncludeSampling());
  }

  /**
   * Checks a dataset can be indexed.
   *
   * @return
   */
  public static ValidationResult checkReadyForIndexing(
      FileSystem fs, String filePath, String datasetId, Integer attempt, boolean includeSampling)
      throws Exception {

    ValidationResult isValid = checkValidationFile(fs, filePath, datasetId, attempt);

    if (!isValid.getValid()) {
      return isValid;
    }

    // check date on DwCA?
    long verbatimTime = metricsModificationTime(fs, filePath, attempt, VERBATIM_METRICS);
    // check date on Interpretation?
    long interpretationTime =
        metricsModificationTime(fs, filePath, attempt, INTERPRETATION_METRICS);
    // check UUID date
    long uuidTime = metricsModificationTime(fs, filePath, attempt, UUID_METRICS);

    if (interpretationTime < verbatimTime) {
      log.warn(
          "The imported verbatim is newer than the interpretation. Interpretation should be re-ran.");
    }
    if (interpretationTime > uuidTime) {
      log.warn(
          "The imported interpretation is newer than the uuid. Unable to index until UUID minting re-ran");
      return ValidationResult.builder().valid(false).message(UUID_REQUIRED).build();
    }

    // check date on UUID ?
    if (includeSampling) {
      // check sampling
      boolean sampleRan = metricsExists(fs, filePath, attempt, SAMPLING_METRICS);
      if (!sampleRan) {
        log.warn("Sampling has not been ran for this dataset. Unable to index dataset.");
        return ValidationResult.builder().valid(false).message(NOT_SAMPLED).build();
      }
      long sampleTime = metricsModificationTime(fs, filePath, attempt, SAMPLING_METRICS);
      if (interpretationTime > sampleTime) {
        log.warn(
            "The sampling needs to be re-ran for this dataset as it pre-dates interpretation data.");
        return ValidationResult.builder().valid(false).message(RESAMPLING_REQUIRED).build();
      }
    }

    return ValidationResult.OK;
  }

  /**
   * Checks the content of the validate file, returning true if the UUID content has been checked
   * and is thought to be valid.
   *
   * @param options
   * @return
   */
  public static ValidationResult checkValidationFile(InterpretationPipelineOptions options)
      throws Exception {

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());

    return checkValidationFile(
        fs, options.getInputPath(), options.getDatasetId(), options.getAttempt());
  }

  public static ValidationResult checkValidationFile(
      FileSystem fs, String inputPath, String datasetId, Integer attempt) throws Exception {

    String validateFilePath = getValidationFilePath(inputPath, datasetId, attempt);
    Path metrics = new Path(validateFilePath);

    if (fs.exists(metrics)) {
      // read YAML
      Yaml yaml = new Yaml();
      // the YAML files created by metrics are UTF-16 encoded
      Map<String, Object> yamlObject =
          yaml.load(new InputStreamReader(fs.open(metrics), StandardCharsets.UTF_8));

      // check invalid record count
      Long emptyKeyRecords =
          Long.parseLong(yamlObject.getOrDefault(EMPTY_KEY_RECORDS, -1L).toString());

      if (emptyKeyRecords > 0) {
        log.warn(
            "The number of records with empty values for all unique terms: " + emptyKeyRecords);
      }

      if (emptyKeyRecords != 0) {
        return ValidationResult.builder().valid(false).message(HAS_EMPTY_KEYS).build();
      }

      // check duplicate record count
      Long duplicateKeyCount =
          Long.parseLong(yamlObject.getOrDefault(DUPLICATE_KEY_COUNT, -1L).toString());
      Long duplicateRecordKeyCount =
          Long.parseLong(yamlObject.getOrDefault(DUPLICATE_RECORD_KEY_COUNT, -1L).toString());

      if (duplicateKeyCount > 0) {
        log.warn("The number of duplicate keys: " + duplicateKeyCount);
        log.warn("The number of records with duplicate keys: " + duplicateRecordKeyCount);
      }

      if (duplicateKeyCount != 0) {
        return ValidationResult.builder().valid(false).message(HAS_DUPLICATES).build();
      } else {
        return ValidationResult.OK;
      }

    } else {
      log.info("Validation not completed  for {}, from inputPath {}", datasetId, validateFilePath);
      return ValidationResult.builder().valid(false).message(NOT_VALIDATED).build();
    }
  }

  /**
   * Checks the content of the validate file, returning true if the UUID content has been checked
   * and is thought to be valid.
   *
   * @param options
   * @return
   */
  public static Long getDuplicateKeyCount(UUIDPipelineOptions options) throws Exception {
    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());
    String validateFilePath = getValidationFilePath(options);
    Path metrics = new Path(validateFilePath);

    if (fs.exists(metrics)) {
      // read YAML
      Yaml yaml = new Yaml();
      // the YAML files created by metrics are UTF-16 encoded
      Map<String, Object> yamlObject =
          yaml.load(new InputStreamReader(fs.open(metrics), StandardCharsets.UTF_8));

      return Long.parseLong(yamlObject.getOrDefault(DUPLICATE_KEY_COUNT, -1L).toString());
    } else {
      throw new FileNotFoundException();
    }
  }

  /**
   * Checks the content of the validate file, returning true if the UUID content has been checked
   * and is thought to be valid.
   *
   * @param options
   * @return
   */
  public static Long getInvalidRecordCount(UUIDPipelineOptions options) throws Exception {

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());
    String validateFilePath = getValidationFilePath(options);
    Path metrics = new Path(validateFilePath);

    if (fs.exists(metrics)) {
      // read YAML
      Yaml yaml = new Yaml();
      // the YAML files created by metrics are UTF-16 encoded
      Map<String, Object> yamlObject =
          yaml.load(new InputStreamReader(fs.open(metrics), StandardCharsets.UTF_8));

      return Long.parseLong(yamlObject.getOrDefault(EMPTY_KEY_RECORDS, -1L).toString());
    } else {
      throw new FileNotFoundException();
    }
  }

  @NotNull
  public static String getValidationFilePath(InterpretationPipelineOptions options) {
    String validateFilePath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId().trim(),
            "1",
            VALIDATION_REPORT_FILE);
    return validateFilePath;
  }

  @NotNull
  public static String getValidationFilePath(String inputPath, String datasetId, Integer attempt) {
    String validateFilePath = String.join("/", inputPath, "1", VALIDATION_REPORT_FILE);
    return validateFilePath;
  }

  public static Path getMetrics(FileSystem fs, String filePath, Integer attempt, String metricsFile)
      throws Exception {
    return new Path(filePath + "/" + attempt + "/" + metricsFile);
  }

  public static Map<String, Object> readValidation(FileSystem fs, String filePath, Integer attempt)
      throws Exception {

    // read YAML
    Yaml yaml = new Yaml();
    Path validationMetrics = getMetrics(fs, filePath, attempt, VALIDATION_REPORT_FILE);

    // the YAML files created by metrics are UTF-16 encoded
    return yaml.load(new InputStreamReader(fs.open(validationMetrics), StandardCharsets.UTF_8));
  }

  public static Long readVerbatimCount(FileSystem fs, String filePath, Integer attempt)
      throws Exception {

    // read YAML
    Yaml yaml = new Yaml();
    Path validationMetrics = getMetrics(fs, filePath, attempt, VERBATIM_METRICS);

    if (!fs.exists(validationMetrics)) {
      return -1l;
    }

    // the YAML files created by metrics are UTF-16 encoded
    Map<String, Object> metrics =
        yaml.load(new InputStreamReader(fs.open(validationMetrics), StandardCharsets.UTF_16));

    return Long.parseLong(metrics.getOrDefault("archiveToErCountAttempted", "-1").toString());
  }

  /**
   * Generate a unique key based on the darwin core fields. This works the same was unique keys
   * where generated in the biocache-store. This is repeated to maintain backwards compatibility
   * with existing data holdings.
   *
   * @param source
   * @param uniqueTerms
   * @return
   * @throws RuntimeException
   */
  public static String generateUniqueKey(
      String datasetID, ExtendedRecord source, List<Term> uniqueTerms) throws RuntimeException {

    List<String> uniqueValues = new ArrayList<String>();
    boolean allUniqueValuesAreEmpty = true;
    for (Term term : uniqueTerms) {
      String value = ModelUtils.extractNullAwareValue(source, term);
      if (value != null && StringUtils.trimToNull(value) != null) {
        // we have a term with a value
        allUniqueValuesAreEmpty = false;
        uniqueValues.add(value.trim());
      }
    }

    if (allUniqueValuesAreEmpty) {

      String termList =
          uniqueTerms.stream().map(t -> t.simpleName()).collect(Collectors.joining(","));
      String errorMessage =
          String.format(
              "Unable to load dataset %s, All supplied unique terms (%s) where empty record with ID %s",
              datasetID, termList, source.getId());

      log.warn(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    // add the datasetID
    uniqueValues.add(0, datasetID);

    // create the unique key
    return String.join(UNIQUE_COMPOSITE_KEY_JOIN_CHAR, uniqueValues);
  }

  /**
   * Generate a unique key based on the darwin core fields. This works the same was unique keys
   * where generated in the biocache-store. This is repeated to maintain backwards compatibility
   * with existing data holdings.
   *
   * @param source
   * @param uniqueTerms
   * @return
   * @throws RuntimeException
   */
  public static String generateUniqueKeyForValidation(
      String datasetID, ExtendedRecord source, List<Term> uniqueTerms) throws RuntimeException {

    final List<String> uniqueValues = new ArrayList<String>();
    boolean allUniqueValuesAreEmpty = true;
    for (Term term : uniqueTerms) {
      String value = ModelUtils.extractNullAwareValue(source, term);
      if (value != null && StringUtils.trimToNull(value) != null) {
        // we have a term with a value
        allUniqueValuesAreEmpty = false;
        uniqueValues.add(value.trim());
      }
    }

    if (allUniqueValuesAreEmpty) {
      return "";
    }

    // add the datasetID
    uniqueValues.add(0, datasetID);

    // create the unique key
    return String.join(UNIQUE_COMPOSITE_KEY_JOIN_CHAR, uniqueValues);
  }

  public static boolean metricsExists(
      FileSystem fs, String filePath, Integer attempt, String metricsFile) throws Exception {
    Path metrics = new Path(String.join("/", filePath, attempt.toString(), metricsFile));
    return fs.exists(metrics);
  }

  public static long metricsModificationTime(
      FileSystem fs, String filePath, Integer attempt, String metricsFile) throws Exception {
    String path = String.join("/", filePath, attempt.toString(), metricsFile);
    Path metrics = new Path(path);
    if (fs.exists(metrics)) {
      return fs.getFileStatus(metrics).getModificationTime();
    } else {
      throw new FileNotFoundException("Unable to read metrics file at: " + path);
    }
  }
}
