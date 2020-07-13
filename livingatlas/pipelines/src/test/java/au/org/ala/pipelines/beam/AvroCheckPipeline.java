package au.org.ala.pipelines.beam;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import java.util.function.Function;

/**
 * Test utility pipelines for testing the outputs of tests.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroCheckPipeline {

    public static void assertExtendedCountRecords(InterpretationPipelineOptions options, Long assertedCount, final Function<ExtendedRecord, Boolean> testFcn) throws Exception {

        log.info("Creating a pipeline from options - loading data from " + options.getInputPath());
        // Initialise pipeline
        Pipeline p = Pipeline.create(options);
        VerbatimTransform verbatimTransform = VerbatimTransform.create();

        PCollection<Long> extendedRecordsCount =
                //filter for records without
                p.apply("Read ExtendedRecords", verbatimTransform.read(options.getInputPath()))
                .apply(Filter.by(new SerializableFunction<ExtendedRecord, Boolean>() {
                    @Override
                    public Boolean apply(ExtendedRecord er) {
                        return testFcn.apply(er);
                    }
                }))
                .apply(Count.globally());
        PAssert.that(extendedRecordsCount).containsInAnyOrder(assertedCount);
        PipelineResult result = p.run();
        result.waitUntilFinish();
    }
}