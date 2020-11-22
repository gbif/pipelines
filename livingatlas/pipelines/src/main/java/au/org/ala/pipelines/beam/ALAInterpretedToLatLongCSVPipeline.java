package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.slf4j.MDC;

/**
 * A pipeline that exports a unique set of coordinates for a dataset into CSV. This pipeline can
 * only be ran after the {@link ALAVerbatimToInterpretedPipeline} has been ran as it relies on the
 * output of the LocationTransform.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAInterpretedToLatLongCSVPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "export-latlng");
    AllDatasetsPipelinesOptions options =
        PipelinesOptionsFactory.create(AllDatasetsPipelinesOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "LAT_LNG_EXPORT");

    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(AllDatasetsPipelinesOptions options) throws Exception {

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    // Initialise pipeline
    Pipeline p = Pipeline.create(options);

    // Use pre-processed coordinates from location transform outputs
    log.info("Adding step 2: Initialise location transform");
    LocationTransform locationTransform = LocationTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollection<String> locationCollection =
        p.apply("Read Location", locationTransform.read(pathFn))
            .apply(Filter.by(lr -> lr.getHasCoordinate()))
            .apply(
                MapElements.via(
                    new SimpleFunction<LocationRecord, String>() {
                      @Override
                      public String apply(LocationRecord input) {
                        return input.getDecimalLatitude() + "," + input.getDecimalLongitude();
                      }
                    }))
            .apply(Distinct.create());

    String outputPath = PathBuilder.buildDatasetAttemptPath(options, "latlng", false);

    // delete previous runs
    FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), outputPath);
    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getTargetPath());
    ALAFsUtils.createDirectory(fs, outputPath);

    locationCollection.apply(TextIO.write().to(outputPath + "/latlong.csv"));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished. Output written to " + outputPath + "/latlong.csv");
  }
}
