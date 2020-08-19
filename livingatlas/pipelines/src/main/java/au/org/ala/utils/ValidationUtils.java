package au.org.ala.utils;

import static au.org.ala.pipelines.beam.ALAUUIDMintingPipeline.UNIQUE_COMPOSITE_KEY_JOIN_CHAR;

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
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

@Slf4j
public class ValidationUtils {

  public static final String DUPLICATE_KEY_COUNT = "duplicateKeyCount";
  public static final String EMPTY_KEY_RECORDS = "emptyKeyRecords";
  public static final String DUPLICATE_RECORD_KEY_COUNT = "duplicateRecordKeyCount";
  public static final String VALIDATION_REPORT_FILE = "validation-report.yaml";
  public static final String VALIDATION_OUTPUT_DIR = "validation";
  public static final String DUPLICATE_KEYS_OUTPUT = "duplicateKeys.csv";
  public static final String UNIQUE_TERMS_SPECIFIED = "uniqueTermsSpecified";

  /**
   * Checks the content of the validate file, returning true if the UUID content has been checked
   * and is thought to be valid.
   *
   * @param options
   * @return
   */
  public static boolean checkValidationFile(UUIDPipelineOptions options) throws Exception {

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

      // check invalid record count
      Long emptyKeyRecords =
          Long.parseLong(yamlObject.getOrDefault(EMPTY_KEY_RECORDS, -1L).toString());

      if (emptyKeyRecords > 0) {
        log.error(
            "The number of records with empty values for all unique terms: " + emptyKeyRecords);
      }

      if (emptyKeyRecords != 0) return false;

      // check duplicate record count
      Long duplicateKeyCount =
          Long.parseLong(yamlObject.getOrDefault(DUPLICATE_KEY_COUNT, -1L).toString());
      Long duplicateRecordKeyCount =
          Long.parseLong(yamlObject.getOrDefault(DUPLICATE_RECORD_KEY_COUNT, -1L).toString());

      if (duplicateKeyCount > 0) {
        log.error("The number of duplicate keys: " + duplicateKeyCount);
        log.error("The number of records with duplicate keys: " + duplicateRecordKeyCount);
      }

      return duplicateKeyCount == 0;

    } else {
      log.error("Enable to read validation file. Has validation pipeline failed ?");
      return false;
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
  public static String getValidationFilePath(UUIDPipelineOptions options) {
    String validateFilePath =
        String.join(
            "/",
            options.getTargetPath(),
            options.getDatasetId().trim(),
            options.getAttempt().toString(),
            VALIDATION_REPORT_FILE);
    return validateFilePath;
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

      log.error(errorMessage);
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
      return "";
    }

    // add the datasetID
    uniqueValues.add(0, datasetID);

    // create the unique key
    return String.join(UNIQUE_COMPOSITE_KEY_JOIN_CHAR, uniqueValues);
  }
}
