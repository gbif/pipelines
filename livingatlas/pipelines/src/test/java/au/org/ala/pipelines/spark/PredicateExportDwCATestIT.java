package au.org.ala.pipelines.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import au.org.ala.pipelines.beam.ALADwcaToVerbatimPipeline;
import au.org.ala.pipelines.beam.ALAInterpretationPipelineOptions;
import au.org.ala.pipelines.beam.ALAInterpretedToSensitivePipeline;
import au.org.ala.pipelines.beam.ALAOccurrenceToSearchAvroPipeline;
import au.org.ala.pipelines.beam.ALAUUIDMintingPipeline;
import au.org.ala.pipelines.beam.ALAVerbatimToInterpretedPipeline;
import au.org.ala.pipelines.options.DwcaToVerbatimPipelineOptions;
import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.DwcaUtils;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** End to end integration tests for exports. */
public class PredicateExportDwCATestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  @BeforeClass
  public static void setupTestData() throws Exception {
    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/event-download"));
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr18391", absolutePath + "/event-download/dr18391");
  }

  /** Tests for SOLR index creation. */
  @Test
  public void testFullExport() throws Exception {
    String jobId = runExport("dr18391");
    String dwcaFilePath = getDwcaFilePath(jobId);

    assertEquals(10, DwcaUtils.countRecordsInCore(dwcaFilePath));
    assertEquals(
        4, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
    assertEquals(
        4,
        DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.MeasurementOrFact.qualifiedName()));
  }

  @NotNull
  private String getDwcaFilePath(String jobId) {
    return "/tmp/la-pipelines-test/event-download/" + jobId + "/dr18391.zip";
  }
  //
  //  @Test
  //  public void testExportByYear() throws Exception {
  //
  //    String absolutePath = new File("src/test/resources").getAbsolutePath();
  //    String jobId = runExport("dr18391", absolutePath +
  // "/event-download/dr18391/query-year.json");
  //    String dwcaFilePath = getDwcaFilePath(jobId);
  //
  //    assertEquals(4, DwcaUtils.countRecordsInCore(dwcaFilePath));
  //    assertEquals(
  //        2, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
  //    assertEquals(
  //        2,
  //        DwcaUtils.countRecordsInExtension(dwcaFilePath,
  // DwcTerm.MeasurementOrFact.qualifiedName()));
  //  }
  //
  //  @Test
  //  public void testExportByMonth() throws Exception {
  //
  //    String absolutePath = new File("src/test/resources").getAbsolutePath();
  //    String jobId = runExport("dr18391", absolutePath +
  // "/event-download/dr18391/query-month.json");
  //    String dwcaFilePath = getDwcaFilePath(jobId);
  //
  //    assertEquals(4, DwcaUtils.countRecordsInCore(dwcaFilePath));
  //    assertEquals(
  //        2, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
  //    assertEquals(
  //        2,
  //        DwcaUtils.countRecordsInExtension(dwcaFilePath,
  // DwcTerm.MeasurementOrFact.qualifiedName()));
  //  }
  //
  //  @Test
  //  public void testExportByStateProvince() throws Exception {
  //
  //    String absolutePath = new File("src/test/resources").getAbsolutePath();
  //    String jobId =
  //        runExport("dr18391", absolutePath + "/event-download/dr18391/query-stateProvince.json");
  //    String dwcaFilePath = getDwcaFilePath(jobId);
  //
  //    assertEquals(4, DwcaUtils.countRecordsInCore(dwcaFilePath));
  //    assertEquals(
  //        2, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
  //    assertEquals(
  //        2,
  //        DwcaUtils.countRecordsInExtension(dwcaFilePath,
  // DwcTerm.MeasurementOrFact.qualifiedName()));
  //  }
  //
  //  @Test
  //  public void testExportByEventTypeHierarchy() throws Exception {
  //
  //    String absolutePath = new File("src/test/resources").getAbsolutePath();
  //    String jobId =
  //        runExport(
  //            "dr18391", absolutePath + "/event-download/dr18391/query-eventTypeHierarchy.json");
  //    String dwcaFilePath = getDwcaFilePath(jobId);
  //
  //    // FIXME need to set eventTypeHierarchy = [eventType] for root
  //    assertEquals(10, DwcaUtils.countRecordsInCore(dwcaFilePath));
  //    assertEquals(
  //        4, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
  //    assertEquals(
  //        4,
  //        DwcaUtils.countRecordsInExtension(dwcaFilePath,
  // DwcTerm.MeasurementOrFact.qualifiedName()));
  //  }
  //
  //  @Test
  //  public void testExportByCompoundQuery() throws Exception {
  //
  //    String absolutePath = new File("src/test/resources").getAbsolutePath();
  //    String jobId =
  //        runExport("dr18391", absolutePath + "/event-download/dr18391/query-compound.json");
  //    String dwcaFilePath = getDwcaFilePath(jobId);
  //
  //    assertEquals(4, DwcaUtils.countRecordsInCore(dwcaFilePath));
  //    assertEquals(
  //        2, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
  //    assertEquals(
  //        2,
  //        DwcaUtils.countRecordsInExtension(dwcaFilePath,
  // DwcTerm.MeasurementOrFact.qualifiedName()));
  //  }
  //
  //  @Test
  //  public void testExportByTaxon() throws Exception {
  //
  //    String absolutePath = new File("src/test/resources").getAbsolutePath();
  //    String jobId = runExport("dr18391", absolutePath +
  // "/event-download/dr18391/query-taxon.json");
  //    String dwcaFilePath = getDwcaFilePath(jobId);
  //
  //    assertEquals(4, DwcaUtils.countRecordsInCore(dwcaFilePath));
  //    assertEquals(
  //        4, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
  //    assertEquals(
  //        4,
  //        DwcaUtils.countRecordsInExtension(dwcaFilePath,
  // DwcTerm.MeasurementOrFact.qualifiedName()));
  //  }
  //
  //  @Test
  //  public void testExportByHigherTaxon() throws Exception {
  //
  //    String absolutePath = new File("src/test/resources").getAbsolutePath();
  //    String jobId =
  //        runExport("dr18391", absolutePath + "/event-download/dr18391/query-higher-taxon.json");
  //    String dwcaFilePath = getDwcaFilePath(jobId);
  //
  //    assertEquals(4, DwcaUtils.countRecordsInCore(dwcaFilePath));
  //    assertEquals(
  //        4, DwcaUtils.countRecordsInExtension(dwcaFilePath, DwcTerm.Occurrence.qualifiedName()));
  //    assertEquals(
  //        4,
  //        DwcaUtils.countRecordsInExtension(dwcaFilePath,
  // DwcTerm.MeasurementOrFact.qualifiedName()));
  //  }

  public static String runExport(String datasetID) {
    String jobId = UUID.randomUUID().toString();
    // run download
    System.setProperty("spark.master", "local[*]");
    PredicateExportDwCAPipeline.main(
        new String[] {
          "--datasetId=" + datasetID,
          "--attempt=1",
          "--jobId=" + jobId,
          "--localExportPath=/tmp/la-pipelines-test/event-download",
          "--inputPath=/tmp/la-pipelines-test/event-download",
          "--config=" + itUtils.getPropertiesFilePath()
        });

    return jobId;
  }

  public static String runExport(String datasetID, String queryFilePath) {
    String jobId = UUID.randomUUID().toString();
    // run download
    System.setProperty("spark.master", "local[*]");
    PredicateExportDwCAPipeline.main(
        new String[] {
          "--datasetId=" + datasetID,
          "--attempt=1",
          "--jobId=" + jobId,
          "--queryFile=" + queryFilePath,
          "--localExportPath=/tmp/la-pipelines-test/event-download",
          "--inputPath=/tmp/la-pipelines-test/event-download",
          "--config=" + itUtils.getPropertiesFilePath()
        });

    return jobId;
  }

  public static void loadTestDataset(String datasetID, String inputPath) throws Exception {

    DwcaToVerbatimPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaToVerbatimPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--runner=DirectRunner",
              "--attempt=1",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/event-download",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--inputPath=" + inputPath
            });
    ALADwcaToVerbatimPipeline.run(dwcaOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--runner=SparkRunner",
              "--attempt=1",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/event-download",
              "--inputPath=/tmp/la-pipelines-test/event-download/dr18391/1/verbatim/*.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    UUIDPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            UUIDPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--runner=DirectRunner",
              "--attempt=1",
              "--metaFileName=" + ValidationUtils.UUID_METRICS,
              "--targetPath=/tmp/la-pipelines-test/event-download",
              "--inputPath=/tmp/la-pipelines-test/event-download",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check validation - should be true as UUIDs are validated and generated
    assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());

    InterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--runner=SparkRunner",
              "--attempt=1",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/event-download",
              "--inputPath=/tmp/la-pipelines-test/event-download",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAInterpretedToSensitivePipeline.run(sensitivityOptions);

    // run event to search AVRO pipeline
    IndexingPipelineOptions avroOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--targetPath=/tmp/la-pipelines-test/event-download",
              "--inputPath=/tmp/la-pipelines-test/event-download",
              "--properties=" + itUtils.getPropertiesFilePath()
            });
    ALAOccurrenceToSearchAvroPipeline.run(avroOptions);
  }
}
