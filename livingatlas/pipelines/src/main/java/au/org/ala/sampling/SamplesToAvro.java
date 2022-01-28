package au.org.ala.sampling;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.pipelines.options.SamplingPipelineOptions;
import au.org.ala.pipelines.util.SamplingUtils;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.*;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.SampleRecord;
import org.slf4j.MDC;

@Slf4j
public class SamplesToAvro {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    CombinedYamlConfiguration conf = new CombinedYamlConfiguration(args);
    String[] combinedArgs = conf.toArgs("general", "sample");
    SamplingPipelineOptions options =
        PipelinesOptionsFactory.create(SamplingPipelineOptions.class, combinedArgs);
    MDC.put("step", "SAMPLING");

    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(SamplingPipelineOptions options) throws Exception {

    int counter = 0;

    // get filesystem
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    // Read CSV
    String sampleCSVDownloadPath = LayerCrawler.getSampleDownloadPath(options);

    if (!ALAFsUtils.exists(fs, sampleCSVDownloadPath)) {
      log.info("No sampling to convert to AVRO. No work to be done.");
      SamplingUtils.writeSamplingMetrics(options, counter, fs);
      return;
    }

    if (sampleCSVDownloadPath.startsWith("hdfs:///")) {
      sampleCSVDownloadPath = sampleCSVDownloadPath.substring(7);
    }

    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(sampleCSVDownloadPath), false);

    while (iter.hasNext()) {

      LocatedFileStatus fileStatus = iter.next();

      if (fileStatus.getPath().getName().endsWith(".csv")) {
        log.info("Reading {} and converting to avro", fileStatus.getPath().getName());
        InputStream inputStream = fs.open(fileStatus.getPath());
        CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream));

        String outputPath = LayerCrawler.getSampleAvroPath(options);
        DatumWriter<SampleRecord> datumWriter =
            new GenericDatumWriter<>(SampleRecord.getClassSchema());

        if (outputPath.startsWith("hdfs:///")) outputPath = outputPath.substring(7);

        try (OutputStream output = fs.create(new Path(outputPath));
            DataFileWriter<SampleRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
          dataFileWriter.setCodec(BASE_CODEC);
          dataFileWriter.create(SampleRecord.getClassSchema(), output);

          String[] columnHeaders = csvReader.readNext();
          String[] line;
          while ((line = csvReader.readNext()) != null) {

            if (line.length == columnHeaders.length) {

              HashMap<String, String> strings = new HashMap<>();
              HashMap<String, Double> doubles = new HashMap<>();

              // first two columns are latitude,longitude
              for (int i = 2; i < columnHeaders.length; i++) {
                if (StringUtils.trimToNull(line[i]) != null) {
                  if (columnHeaders[i].startsWith("el")) {
                    try {
                      doubles.put(columnHeaders[i], Double.parseDouble(line[i]));
                    } catch (NumberFormatException ex) {
                      // do something
                    }
                  } else {
                    strings.put(columnHeaders[i], line[i]);
                  }
                }
              }

              SampleRecord sampleRecord =
                  SampleRecord.newBuilder()
                      .setLatLng(line[0] + "," + line[1])
                      .setDoubles(doubles)
                      .setStrings(strings)
                      .build();
              dataFileWriter.append(sampleRecord);
              counter = +1;
            }
          }
        }
        log.info("File written to {}", outputPath);
      }
    }

    if (!options.getKeepSamplingDownloads()) {
      log.info("Deleting sampling CSV downloads.");
      ALAFsUtils.deleteIfExist(fs, sampleCSVDownloadPath);
      log.info("Deleted.");
    } else {
      log.info("Keeping sampling CSV downloads.");
    }

    SamplingUtils.writeSamplingMetrics(options, counter, fs);
    log.info("Conversion to avro complete.");
  }
}
