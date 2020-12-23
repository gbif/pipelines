package au.org.ala.sampling;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
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
    AllDatasetsPipelinesOptions options =
        PipelinesOptionsFactory.create(AllDatasetsPipelinesOptions.class, combinedArgs);
    MDC.put("step", "SAMPLING");

    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(AllDatasetsPipelinesOptions options) throws Exception {

    // get filesystem
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    // Read CSV
    String samplePath = LayerCrawler.getSampleDownloadPath(options);

    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(samplePath), false);

    while (iter.hasNext()) {

      LocatedFileStatus fileStatus = iter.next();

      if (fileStatus.getPath().getName().endsWith(".csv")) {
        log.info("Reading {} and converting to avro", fileStatus.getPath().getName());
        InputStream inputStream = fs.open(fileStatus.getPath());
        CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream));

        String outputPath = LayerCrawler.getSampleAvroPath(options);
        OutputStream output = fs.create(new Path(outputPath));
        DatumWriter<SampleRecord> datumWriter =
            new GenericDatumWriter<>(SampleRecord.getClassSchema());
        DataFileWriter dataFileWriter = new DataFileWriter<SampleRecord>(datumWriter);
        dataFileWriter.setCodec(BASE_CODEC);
        dataFileWriter.create(SampleRecord.getClassSchema(), output);

        String[] columnHeaders = csvReader.readNext();
        String[] line = new String[0];
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
          }
        }
        dataFileWriter.close();
        log.info("File written to {}", outputPath);
      }
    }
    log.info("Conversion to avro complete.");
  }
}
