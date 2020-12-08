package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.slf4j.MDC;

/**
 * Exports a unique set of coordinates for all datasets. This tool is dependent on globstars being
 * supported on the host OS.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExportAllLatLongCSVPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "export-latlng");
    AllDatasetsPipelinesOptions options =
        PipelinesOptionsFactory.create(AllDatasetsPipelinesOptions.class, combinedArgs);
    MDC.put("datasetId", "ALL_DATASETS");
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "LAT_LNG_EXPORT_ALL");

    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(AllDatasetsPipelinesOptions options) throws Exception {

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getTargetPath());

    // delete and re-create output directory
    ALAFsUtils.deleteIfExist(fs, options.getAllDatasetsInputPath() + "/latlng/");
    ALAFsUtils.createDirectory(fs, options.getAllDatasetsInputPath() + "/latlng/");

    Pipeline p = Pipeline.create(options);
    p.apply(
            AvroIO.read(IndexRecord.class)
                .from(
                    String.join(
                        "/", options.getAllDatasetsInputPath(), "index-record", "*/*.avro")))
        .apply(Filter.by(ir -> ir.getLatLng() != null))
        .apply(
            MapElements.via(
                new SimpleFunction<IndexRecord, String>() {
                  @Override
                  public String apply(IndexRecord input) {
                    return input.getLatLng();
                  }
                }))
        .apply(Distinct.create())
        .apply(
            TextIO.write()
                .to(options.getAllDatasetsInputPath() + "/latlng/latlng.csv")
                .withoutSharding());

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info(
        "Pipeline has been finished. Output written to {}/latlng/latlong.csv",
        options.getAllDatasetsInputPath());
  }
}
