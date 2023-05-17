package au.org.ala.utils;

import static org.junit.Assert.*;

import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.util.TestUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Test;

public class ValidationUtilsTest {

  @Test
  public void testIsNotValidNoDir() throws Exception {

    // setup files
    IndexingPipelineOptions indexingOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=dr123",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/validation-test",
              "--inputPath=/tmp/la-pipelines-test/validation-test",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--includeSensitiveDataChecks=true",
              "--includeImages=false"
            });
    ValidationResult validationResult = ValidationUtils.checkReadyForIndexing(indexingOptions);
    assertFalse(validationResult.getValid());
  }

  @Test
  public void testIsNotValidEmpty() throws Exception {

    // setup files
    FileUtils.forceMkdir(new File("/tmp/la-pipelines-test/validation-test/dr123"));
    IndexingPipelineOptions indexingOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=dr123",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/validation-test",
              "--inputPath=/tmp/la-pipelines-test/validation-test",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--includeSensitiveDataChecks=true",
              "--includeImages=false"
            });
    ValidationResult validationResult = ValidationUtils.checkReadyForIndexing(indexingOptions);
    assertFalse(validationResult.getValid());
  }

  @Test
  public void testIsValid() throws Exception {

    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/validation-test"));
    File datasetDir = new File("/tmp/la-pipelines-test/validation-test/dr124/1");
    FileUtils.forceMkdir(datasetDir);

    // setup files
    FileUtils.touch(new File("/tmp/la-pipelines-test/validation-test/dr124/1/dwca-metrics.yml"));
    FileUtils.touch(
        new File("/tmp/la-pipelines-test/validation-test/dr124/1/interpretation-metrics.yml"));
    FileUtils.touch(
        new File("/tmp/la-pipelines-test/validation-test/dr124/1/sensitive-metrics.yml"));
    File validationReport = new File("src/test/resources/validation-test/validation-report.yaml");
    FileUtils.copyFileToDirectory(validationReport, datasetDir);
    FileUtils.touch(new File("/tmp/la-pipelines-test/validation-test/dr124/1/uuid-metrics.yml"));

    IndexingPipelineOptions indexingOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=dr124",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/validation-test",
              "--inputPath=/tmp/la-pipelines-test/validation-test",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--includeSensitiveDataChecks=true",
              "--includeImages=false"
            });
    ValidationResult validationResult = ValidationUtils.checkReadyForIndexing(indexingOptions);
    assertTrue(validationResult.getValid());
  }

  @Test
  public void testIsValidWithTimeBuffer() throws Exception {

    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/validation-test"));
    File datasetDir = new File("/tmp/la-pipelines-test/validation-test/dr124/1");
    FileUtils.forceMkdir(datasetDir);

    // setup files
    FileUtils.touch(new File("/tmp/la-pipelines-test/validation-test/dr124/1/dwca-metrics.yml"));

    FileUtils.touch(
        new File("/tmp/la-pipelines-test/validation-test/dr124/1/sensitive-metrics.yml"));
    File validationReport = new File("src/test/resources/validation-test/validation-report.yaml");
    FileUtils.copyFileToDirectory(validationReport, datasetDir);
    FileUtils.touch(new File("/tmp/la-pipelines-test/validation-test/dr124/1/uuid-metrics.yml"));

    Thread.sleep(1000);
    FileUtils.touch(
        new File("/tmp/la-pipelines-test/validation-test/dr124/1/interpretation-metrics.yml"));

    IndexingPipelineOptions indexingOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=dr124",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/validation-test",
              "--inputPath=/tmp/la-pipelines-test/validation-test",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--includeSensitiveDataChecks=true",
              "--timeBufferInMillis=2000",
              "--includeImages=false"
            });
    ValidationResult validationResult = ValidationUtils.checkReadyForIndexing(indexingOptions);
    assertTrue(validationResult.getValid());
  }

  @Test
  public void testIsNotValidWithTimeBuffer() throws Exception {

    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/validation-test"));
    File datasetDir = new File("/tmp/la-pipelines-test/validation-test/dr124/1");
    FileUtils.forceMkdir(datasetDir);

    // setup files
    FileUtils.touch(new File("/tmp/la-pipelines-test/validation-test/dr124/1/dwca-metrics.yml"));

    FileUtils.touch(
        new File("/tmp/la-pipelines-test/validation-test/dr124/1/sensitive-metrics.yml"));
    File validationReport = new File("src/test/resources/validation-test/validation-report.yaml");
    FileUtils.copyFileToDirectory(validationReport, datasetDir);
    FileUtils.touch(new File("/tmp/la-pipelines-test/validation-test/dr124/1/uuid-metrics.yml"));

    Thread.sleep(3000);
    FileUtils.touch(
        new File("/tmp/la-pipelines-test/validation-test/dr124/1/interpretation-metrics.yml"));

    IndexingPipelineOptions indexingOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=dr124",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/validation-test",
              "--inputPath=/tmp/la-pipelines-test/validation-test",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--includeSensitiveDataChecks=true",
              "--timeBufferInMillis=1000",
              "--includeImages=false"
            });
    ValidationResult validationResult = ValidationUtils.checkReadyForIndexing(indexingOptions);
    assertFalse(validationResult.getValid());
  }
}
