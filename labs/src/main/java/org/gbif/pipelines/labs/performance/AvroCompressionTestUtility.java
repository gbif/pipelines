package org.gbif.pipelines.labs.performance;

import org.gbif.pipelines.labs.performance.avro.DwCToAvroDatasetFunction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It is a utility to run avro compression test. This utility generates a csv file compressionTestResult.csv with all the readings.
 */
public class AvroCompressionTestUtility {

  private static final Logger LOG = LoggerFactory.getLogger(AvroCompressionTestUtility.class);

  static void runCompressionTest(String basePath, String resultPath, int repetition, Integer[] syncIntervals)
    throws IOException {
    //configuring dwca dataset path (make sure DwC dataset are expanded)
    Path[] datasets = testingDatasets(basePath);

    //deflate,no codecs and snappy codec
    CodecFactory[] codecs = testingCodecs();

    //create compression test and fetch result
    List<CompressionResult> compressionResults = CompressionTestBuilder.withDatasets(datasets)
      .withEach(codecs)
      .withSyncIntervals(syncIntervals)
      .times(repetition)
      .performTestUsing(new DwCToAvroDatasetFunction());

    //dump the result in a file
    StringJoiner joiner = new StringJoiner(System.lineSeparator());
    joiner.add(
      "Dataset,syncInterval,repetition,original file size(in bytes),compressed file size(in bytes),formatted original file size,formatted compressed file size,read time (in ms),write time (in ms),codec,number of occurrence");
    compressionResults.forEach(result -> joiner.add(result.toCSV()));

    Files.write(new File(resultPath).toPath(), joiner.toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get list of dexpanded dwca data sets from the provided base folder
   * @param basePath
   * @return
   */
  private static Path[] testingDatasets(String basePath) {
    return Arrays.stream(Paths.get(basePath).toFile().listFiles(File::isDirectory))
      .map(file -> Paths.get(file.getPath()))
      .toArray(Path[]::new);
  }

  /**
   * Get list of deflate,snappy and null codecs
   */
  private static CodecFactory[] testingCodecs() {
    List<CodecFactory> factories = IntStream.rangeClosed(1, 9).mapToObj(CodecFactory::deflateCodec).collect(Collectors.toList());
    factories.add(CodecFactory.snappyCodec());
    factories.add(CodecFactory.nullCodec());
    return factories.toArray(new CodecFactory[factories.size()]);
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      LOG.error(
        "Usage java -jar labs.jar org.gbif.pipelines.labs.performance.AvroCompressionTestUtility /path/to/dataset /path/to/result.csv 2");
      System.exit(1);
    }
    Integer[] syncIntervals = {128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2048 * 1024};
    runCompressionTest(args[0], args[1], Integer.parseInt(args[2]), syncIntervals);
  }

}
