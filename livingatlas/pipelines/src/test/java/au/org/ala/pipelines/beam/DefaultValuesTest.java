package au.org.ala.pipelines.beam;

import org.codehaus.plexus.util.FileUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.function.Function;

/**
 * End to end default values test.
 */
public class DefaultValuesTest {

    @Test
    public void testDwCaPipeline() throws Exception {

        //clear up previous test runs
        FileUtils.forceDelete("/tmp/la-pipelines-test/default-values");

        String absolutePath = new File("src/test/resources").getAbsolutePath();

        DwcaPipelineOptions dwcaOptions = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, new String[]{
                "--datasetId=dr893",
                "--attempt=1",
                "--runner=SparkRunner",
                "--metaFileName=dwca-metrics.yml",
                "--targetPath=/tmp/la-pipelines-test/default-values",
                "--inputPath=" + absolutePath + "/default-values/dr893"
        });
        DwcaToVerbatimPipeline.run(dwcaOptions);

        //check the original verbatim values are NOT populated with default values
        InterpretationPipelineOptions testOptions1 = PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, new String[]{
                "--datasetId=dr893",
                "--attempt=1",
                "--runner=SparkRunner",
                "--metaFileName=uuid-metrics.yml",
                "--targetPath=/tmp/la-pipelines-test/default-values",
                "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/verbatim.avro",
                "--properties=src/test/resources/pipelines.yaml",
                "--useExtendedRecordId=true"
        });
        Function<ExtendedRecord, Boolean> notPopulated = (Function<ExtendedRecord, Boolean> & Serializable) er ->
                er.getCoreTerms().get(DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name()) == null
                    && er.getCoreTerms().get(DwcTerm.occurrenceStatus.namespace() + DwcTerm.basisOfRecord.name()) == null;
        AvroCheckPipeline.assertExtendedCountRecords(testOptions1, 5l, notPopulated);

        //Run the interpretation pipeline
        InterpretationPipelineOptions interpretationOptions = PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, new String[]{
                "--datasetId=dr893",
                "--attempt=1",
                "--runner=SparkRunner",
                "--interpretationTypes=ALL",
                "--metaFileName=interpretation-metrics.yml",
                "--targetPath=/tmp/la-pipelines-test/default-values",
                "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/verbatim.avro",
                "--properties=src/test/resources/pipelines.yaml",
                "--useExtendedRecordId=true"
        });
        ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

        //check the interpreted values are NOW populated with default values
        InterpretationPipelineOptions checkPopulatedOptions = PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, new String[]{
                "--datasetId=dr893",
                "--attempt=1",
                "--runner=SparkRunner",
                "--targetPath=/tmp/la-pipelines-test/default-values",
                "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/interpreted/verbatim/interpret-*",
                "--properties=src/test/resources/pipelines.yaml"
        });

        //check default values are populated
        Function<ExtendedRecord, Boolean> checkPopulatedFcn = (Function<ExtendedRecord, Boolean> & Serializable) er ->
                   er.getCoreTerms().containsKey(DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name())
                && er.getCoreTerms().containsKey(DwcTerm.occurrenceStatus.namespace() + DwcTerm.occurrenceStatus.name())
                && er.getCoreTerms().get(DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name()).equals("HumanObservation")
                && er.getCoreTerms().get(DwcTerm.occurrenceStatus.namespace() + DwcTerm.occurrenceStatus.name()).equals("present");

        AvroCheckPipeline.assertExtendedCountRecords(checkPopulatedOptions, 5l, checkPopulatedFcn);
    }
}
