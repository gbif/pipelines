package au.org.ala.pipelines.beam;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.transforms.RecordAnnotationTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.slf4j.MDC;

/**
 * Pipeline that reads DwCAs with annotations (occurrence core) and creates RecordAnnotation AVRO
 * files
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AnnotationPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "annotation");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.ANNOTATION_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) throws Exception {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.DWCA_TO_VERBATIM.name());

    log.info("Adding step 1: Options");
    String inputPath = options.getInputPath();

    final HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    final ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(hdfsConfigs, options.getProperties()).get();

    final ALACollectoryMetadata m =
        ALAAttributionKVStoreFactory.create(config).get(options.getDatasetId());

    if (Objects.equals(m, ALACollectoryMetadata.EMPTY)) {
      log.error(
          "Collectory metadata no available for {}. Will not run interpretation",
          options.getDatasetId());
      return;
    }

    RecordAnnotationTransform recordAnnotationTransform =
        RecordAnnotationTransform.builder()
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .datasetId(options.getDatasetId())
            .create();

    log.info("Adding step 2: Pipeline steps");
    Pipeline p = Pipeline.create(options);

    Map<String, String> files =
        ALAFsUtils.listAllDatasets(hdfsConfigs, inputPath + "/" + options.getDatasetId());

    PCollection<ExtendedRecord> annotations = null;

    if (files.size() > 1) {

      PCollectionList<ExtendedRecord> pcs = PCollectionList.<ExtendedRecord>empty(p);

      for (String filePath : files.values()) {
        if (filePath.endsWith(".zip")) {
          String localPath = convertToLocal(options, filePath);
          DwcaIO.Read reader =
              DwcaIO.Read.fromCompressed(
                  localPath,
                  options.getTempLocation() + "/annotations-" + System.currentTimeMillis());
          PCollection<ExtendedRecord> pc = p.apply("Read from Darwin Core Archive", reader);
          pcs = pcs.and(pc);
        }
      }

      annotations = pcs.apply(Flatten.<ExtendedRecord>pCollections());
    } else if (files.size() == 1) {

      String filePath = files.values().iterator().next();
      String localPath = convertToLocal(options, filePath);
      DwcaIO.Read reader1 =
          DwcaIO.Read.fromCompressed(
              localPath, options.getTempLocation() + "/annotations-" + System.currentTimeMillis());
      annotations = p.apply("Read from Darwin Core Archive", reader1);
    } else {
      log.info("No annotations files found in " + inputPath + "/" + options.getDatasetId());
      return;
    }

    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(options.getTargetPath());
    Path path = ALAFsUtils.createPath(options.getTargetPath());

    String targetOutputPath =
        String.join(
            "/",
            fs.getFileStatus(path).getPath().toString(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "annotation");

    log.info("Writing output to " + targetOutputPath);
    annotations
        .apply("Write to avro", recordAnnotationTransform.interpret())
        .apply("Write location to avro", recordAnnotationTransform.write(targetOutputPath));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  public static String convertToLocal(InterpretationPipelineOptions options, String inputPath)
      throws IOException {

    log.info("HDFS Input path: {}", inputPath);
    FileSystem fs =
        FileSystemFactory.getInstance(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()))
            .getFs(options.getInputPath());

    Path inputPathHdfs = new Path(inputPath);

    if (!fs.exists(inputPathHdfs)) {
      throw new RuntimeException("Input file not available: " + inputPath);
    }

    FileUtils.forceMkdir(new File(options.getTempLocation()));
    String tmpLocalFilePath = options.getTempLocation() + "/" + inputPathHdfs.getName();

    log.info("Copy from HDFS to local FS: {}", tmpLocalFilePath);

    Path tmpPath = new Path(tmpLocalFilePath);
    fs.copyToLocalFile(false, inputPathHdfs, tmpPath, true);

    return tmpLocalFilePath;
  }
}
