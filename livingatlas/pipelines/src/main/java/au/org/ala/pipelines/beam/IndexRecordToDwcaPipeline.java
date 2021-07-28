package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.beam.utils.PathBuilder.buildDatasetAttemptPath;
import static org.gbif.pipelines.common.beam.utils.PathBuilder.buildPath;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.converters.CoreCsvConverter;
import au.org.ala.pipelines.converters.MultimediaCsvConverter;
import au.org.ala.pipelines.options.SolrPipelineOptions;
import au.org.ala.pipelines.util.DwcaMetaXml;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.Scanner;
import java.util.function.UnaryOperator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.slf4j.MDC;

@Slf4j
public class IndexRecordToDwcaPipeline {

  public static void main(String[] args) throws Exception {
    MDC.put("step", "INDEX_RECORD_TO_DWCA");
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "solr");
    SolrPipelineOptions options =
        PipelinesOptionsFactory.create(SolrPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId() != null ? options.getDatasetId() : "ALL_RECORDS");
    options.setMetaFileName(ValidationUtils.VERBATIM_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  @SneakyThrows
  public static void run(SolrPipelineOptions options) {

    UnaryOperator<String> pathFn =
        fileName -> buildPath(buildDatasetAttemptPath(options, "dwca", false), fileName).toString();

    Pipeline p = Pipeline.create(options);

    // Load IndexRecords - keyed on UUID
    PCollection<IndexRecord> indexRecordPCollection = ALAFsUtils.loadIndexRecords(options, p);

    indexRecordPCollection
        .apply(
            "Convert to core csv string",
            MapElements.into(TypeDescriptors.strings()).via(CoreCsvConverter::convert))
        .apply(
            "Write core csv file",
            TextIO.write().to(pathFn.apply("occurrence")).withoutSharding().withSuffix(".csv"));

    indexRecordPCollection
        .apply(
            "Convert to image csv string",
            MapElements.into(TypeDescriptors.strings()).via(MultimediaCsvConverter::convert))
        .apply(
            "Write image csv file",
            TextIO.write().to(pathFn.apply("image")).withoutSharding().withSuffix(".csv"));

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
  }

  @SneakyThrows
  private static void writeEML(SolrPipelineOptions options, UnaryOperator<String> pathFn) {
    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getProperties())
            .get();
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

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
