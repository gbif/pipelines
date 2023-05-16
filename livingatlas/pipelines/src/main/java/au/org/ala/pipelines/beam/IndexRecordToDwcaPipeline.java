package au.org.ala.pipelines.beam;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.gbif.pipelines.common.beam.utils.PathBuilder.buildDatasetAttemptPath;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.converters.CoreCsvConverter;
import au.org.ala.pipelines.converters.MultimediaCsvConverter;
import au.org.ala.pipelines.options.DwCAExportPipelineOptions;
import au.org.ala.pipelines.util.DwcaMetaXml;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import java.util.Scanner;
import java.util.function.UnaryOperator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.slf4j.MDC;

@Slf4j
public class IndexRecordToDwcaPipeline {

  public static void main(String[] args) throws Exception {
    MDC.put("step", "INDEX_RECORD_TO_DWCA");
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "export");
    DwCAExportPipelineOptions options =
        PipelinesOptionsFactory.create(DwCAExportPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId() != null ? options.getDatasetId() : "ALL_RECORDS");
    options.setMetaFileName(ValidationUtils.VERBATIM_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  @SneakyThrows
  public static void run(DwCAExportPipelineOptions options) throws Exception {

    UnaryOperator<String> pathFn =
        fileName ->
            String.join(Path.SEPARATOR, buildDatasetAttemptPath(options, "dwca", false), fileName);

    Pipeline p = Pipeline.create(options);

    // Load IndexRecords - keyed on UUID
    PCollection<IndexRecord> indexRecordPCollection = ALAFsUtils.loadIndexRecords(options, p);

    indexRecordPCollection
        .apply(
            "Convert to core csv string",
            MapElements.into(strings()).via(CoreCsvConverter::convert))
        .apply(
            "Write core csv file",
            TextIO.write().to(pathFn.apply("occurrence")).withoutSharding().withSuffix(".tsv"));

    final String formatPath = options.getImageServicePath();

    indexRecordPCollection
        .apply(
            MapElements.via(
                new SimpleFunction<IndexRecord, List<String>>() {
                  @Override
                  public List<String> apply(IndexRecord indexRecord) {
                    return MultimediaCsvConverter.convert(indexRecord, formatPath);
                  }
                }))
        .apply(Flatten.iterables())
        .apply(
            "Write image csv file",
            TextIO.write().to(pathFn.apply("image")).withoutSharding().withSuffix(".tsv"));

    PipelineResult result = p.run();
    result.waitUntilFinish();

    // write the meta.xml
    DwcaMetaXml.builder()
        .coreTerms(CoreCsvConverter.getTerms())
        .multimediaTerms(MultimediaCsvConverter.getTerms())
        .pathToWrite(pathFn.apply("meta.xml"))
        .coreSiteConfig(options.getCoreSiteConfig())
        .hdfsSiteConfig(options.getHdfsSiteConfig())
        .create()
        .write();

    // Write the eml.xml
    writeEML(options, pathFn);

    // create output dir
    FileUtils.forceMkdir(new File(options.getLocalExportPath()));

    // copy from HDFS to /tmp
    boolean originalInputIsHdfs = options.getInputPath().startsWith("hdfs://");

    // if inputPath is "hdfs://", then copy to local
    if (originalInputIsHdfs) {

      FileSystem fs =
          FileSystemFactory.getInstance(
                  HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()))
              .getFs(options.getInputPath());

      Path inputPathHdfs = new Path(options.getInputPath());

      if (!fs.exists(inputPathHdfs)) {
        throw new RuntimeException("Input file not available: " + options.getInputPath());
      }

      String dwcaHdfsOutputPath = buildDatasetAttemptPath(options, "dwca", false);
      String dwcaOutputPath = options.getLocalExportPath() + "/" + options.getDatasetId() + "/";
      RemoteIterator<LocatedFileStatus> iter =
          fs.listFiles(new Path(dwcaHdfsOutputPath + "/"), false);

      while (iter.hasNext()) {
        LocatedFileStatus locatedFileStatus = iter.next();
        Path path = locatedFileStatus.getPath();
        if (fs.isFile(path)) {
          log.info("Transferring " + path.toString() + " to " + dwcaOutputPath);
          fs.copyToLocalFile(false, path, new Path(dwcaOutputPath + "/" + path.getName()), false);
        }
      }
    }

    String zipPath = options.getLocalExportPath() + "/" + options.getDatasetId() + ".zip";
    String dwcaOutputPath = options.getLocalExportPath() + "/" + options.getDatasetId();
    File[] filePaths =
        originalInputIsHdfs
            ? new File(dwcaOutputPath).listFiles()
            : new File(buildDatasetAttemptPath(options, "dwca", false)).listFiles();

    try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipPath))) {
      for (File fileToZip : filePaths) {
        log.info("Adding to Zip file: " + fileToZip.getName());
        zipOut.putNextEntry(new ZipEntry(fileToZip.getName()));
        Files.copy(fileToZip.toPath(), zipOut);
      }
    }
    log.info("Zip file written to: " + zipPath);
  }

  @SneakyThrows
  private static void writeEML(DwCAExportPipelineOptions options, UnaryOperator<String> pathFn) {
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(hdfsConfigs, options.getProperties()).get();
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, options.getInputPath());

    String url = config.getCollectory().getWsUrl() + "/eml/" + options.getDatasetId();
    URL emlUrl = new URL(url);
    try (InputStream input = emlUrl.openStream();
        OutputStream output = ALAFsUtils.openOutputStream(fs, pathFn.apply("eml.xml"));
        OutputStreamWriter writer = new OutputStreamWriter(output)) {
      String out = new Scanner(input, "UTF-8").useDelimiter("\\A").next();
      writer.write(out);
    }
  }
}
