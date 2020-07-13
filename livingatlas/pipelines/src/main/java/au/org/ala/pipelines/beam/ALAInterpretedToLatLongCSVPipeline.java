package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.transforms.ALACSVDocumentTransform;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.slf4j.MDC;
import java.util.function.UnaryOperator;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

/**
 * A pipeline that exports a unique set of coordinates for a dataset into CSV.
 * This pipeline can only be ran after the {@link ALAVerbatimToInterpretedPipeline} has been ran
 * as it relies on the output of the LocationTransform.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAInterpretedToLatLongCSVPipeline {

    public static void main(String[] args) throws Exception {
        String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "export-latlng");
        InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(combinedArgs);
        run(options);
    }

    public static void run(InterpretationPipelineOptions options) throws Exception {

        MDC.put("datasetId", options.getDatasetId());
        MDC.put("attempt", options.getAttempt().toString());

        log.info("Adding step 1: Options");
        UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

        // Initialise pipeline
        Pipeline p = Pipeline.create(options);

        // Use pre-processed coordinates from location transform outputs
        log.info("Adding step 2: Initialise location transform");
        LocationTransform locationTransform = LocationTransform.builder().create();

        log.info("Adding step 3: Creating beam pipeline");
        PCollection<KV<String, LocationRecord>> locationCollection =
                p.apply("Read Location", locationTransform.read(pathFn))
                        .apply("Map Location to KV", locationTransform.toKv());

        log.info("Adding step 3: Converting into a CSV object");
        ParDo.SingleOutput<KV<String, CoGbkResult>, String> alaCSVrDoFn =
                ALACSVDocumentTransform.create(locationTransform.getTag()).converter();

        PCollection<String> csvCollection =
                KeyedPCollectionTuple
                        .of(locationTransform.getTag(), locationCollection)
                        .apply("Grouping objects", CoGroupByKey.create())
                        .apply("Merging to CSV doc", alaCSVrDoFn);

        String outputPath = FsUtils.buildDatasetAttemptPath(options, "latlng", true);

        //delete previous runs
        FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), outputPath);
        FileSystem fs = FileSystemFactory.getInstance(options.getHdfsSiteConfig()).getFs("/");
        ALAFsUtils.createDirectory(fs, outputPath);

        csvCollection
                .apply(Distinct.<String>create())
                .apply(TextIO.write().to(outputPath + "/latlong.csv"));

        log.info("Running the pipeline");
        PipelineResult result = p.run();
        result.waitUntilFinish();

        MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

        log.info("Pipeline has been finished. Output written to " + outputPath + "/latlong.csv");
    }
}