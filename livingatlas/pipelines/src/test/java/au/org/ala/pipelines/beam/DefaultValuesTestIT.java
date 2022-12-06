package au.org.ala.pipelines.beam;

import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.io.Serializable;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.ClassRule;
import org.junit.Test;

/** End to end default values test. */
public class DefaultValuesTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  @Test
  public void testDwCaPipeline() {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/default-values"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Load test DwC archive
    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=" + absolutePath + "/default-values/dr893"
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // check the original verbatim values are NOT populated with default values
    InterpretationPipelineOptions testOptions1 =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });

    // validate that the raw values for basisOfRecord and occurrenceStatus are current null
    Function<ExtendedRecord, Boolean> notPopulated =
        (Function<ExtendedRecord, Boolean> & Serializable)
            er ->
                er.getCoreTerms()
                            .get(DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name())
                        == null
                    && er.getCoreTerms()
                            .get(
                                DwcTerm.occurrenceStatus.namespace() + DwcTerm.basisOfRecord.name())
                        == null;
    AvroCheckPipeline.assertExtendedCountRecords(testOptions1, 5L, notPopulated);

    // Run the interpretation pipeline
    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // check the interpreted values are NOW populated with default values
    InterpretationPipelineOptions checkPopulatedOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/occurrence/verbatim/interpret-*",
              "--properties=" + itUtils.getPropertiesFilePath()
            });

    // check default values are populated
    Function<ExtendedRecord, Boolean> checkPopulatedFcn =
        (Function<ExtendedRecord, Boolean> & Serializable)
            er ->
                er.getCoreTerms()
                        .containsKey(
                            DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name())
                    && er.getCoreTerms()
                        .containsKey(
                            DwcTerm.occurrenceStatus.namespace() + DwcTerm.occurrenceStatus.name())
                    && er.getCoreTerms()
                        .get(DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name())
                        .equals("HumanObservation")
                    && er.getCoreTerms()
                        .get(DwcTerm.occurrenceStatus.namespace() + DwcTerm.occurrenceStatus.name())
                        .equals("present");

    AvroCheckPipeline.assertExtendedCountRecords(checkPopulatedOptions, 5L, checkPopulatedFcn);
  }
}
