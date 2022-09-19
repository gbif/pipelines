package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import au.org.ala.pipelines.options.SpeciesLevelPipelineOptions;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.util.Map;
import okhttp3.mockwebserver.MockWebServer;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.TaxonProfile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SpeciesListPipelineIT {

  MockWebServer collectoryServer;
  MockWebServer speciesListServer;

  @Before
  public void setup() throws Exception {
    speciesListServer = TestUtils.createMockSpeciesLists();
    speciesListServer.start(TestUtils.getSpeciesListPort());

    collectoryServer = TestUtils.createMockCollectory();
    collectoryServer.start(TestUtils.getCollectoryPort());
  }

  /** Tests for SOLR index creation. */
  @Test
  public void testSpeciesListPipeline() throws Exception {
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    loadTestDataset("dr893", absolutePath + "/species-lists/dr893");

    Map<String, TaxonProfile> tps =
        AvroReader.readRecords(
            HdfsConfigs.nullConfig(),
            TaxonProfile.class,
            "/tmp/la-pipelines-test/species-lists/dr893/1/taxon_profiles/*.avro");

    assertTrue(tps.get("not-an-uuid-1").getSpeciesListID().contains("dr1"));
    assertEquals(2, tps.get("not-an-uuid-1").getSpeciesListID().size());
    assertEquals(1, tps.get("not-an-uuid-2").getSpeciesListID().size());

    assertEquals(1, tps.get("not-an-uuid-3").getSpeciesListID().size());
    assertEquals(1, tps.get("not-an-uuid-4").getSpeciesListID().size());

    assertEquals(
        "Endangered", tps.get("not-an-uuid-1").getConservationStatuses().get(0).getStatus());
  }

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/species-lists",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    InterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/species-lists",
              "--inputPath=/tmp/la-pipelines-test/species-lists/dr893/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    SpeciesLevelPipelineOptions speciesLevelPipelineOptions =
        PipelinesOptionsFactory.create(
            SpeciesLevelPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/species-lists",
              "--inputPath=/tmp/la-pipelines-test/species-lists",
              "--speciesAggregatesPath=/tmp/la-pipelines-test/",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    SpeciesListPipeline.run(speciesLevelPipelineOptions);
  }

  @After
  public void teardown() throws Exception {
    speciesListServer.shutdown();
    collectoryServer.shutdown();
  }
}
