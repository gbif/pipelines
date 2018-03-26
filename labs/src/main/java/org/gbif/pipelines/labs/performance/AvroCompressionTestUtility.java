package org.gbif.pipelines.labs.performance;

import org.gbif.pipelines.labs.performance.avro.DwCToAvroDatasetFunction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    Path[] datasets;
    try (Stream<Path> pathStream = Files.list(Paths.get(basePath))) {
      datasets =
        pathStream.filter((path) -> path.toFile().isDirectory()).collect(Collectors.toList()).toArray(new Path[] {});
    }
    //deflate,no codecs and snappy codec
    Supplier<List<CodecFactory>> codecFactorySupplier = () -> {
      List<CodecFactory> factories = new ArrayList<>();
      for (int i = 1; i <= 9; i++) {
        factories.add(CodecFactory.deflateCodec(i));
      }
      factories.addAll(Arrays.asList(CodecFactory.snappyCodec(), CodecFactory.nullCodec()));
      return factories;
    };

    CodecFactory[] codecs = codecFactorySupplier.get().toArray(new CodecFactory[] {});
    //create compression test and fetch result
    List<CompressionResult> compressionResults = CompressionTestBuilder.forAll(datasets)
      .withEach(codecs)
      .forEach(syncIntervals)
      .times(repetition)
      .performTestUsing(new DwCToAvroDatasetFunction());
    //dump the result in a file
    StringBuilder buffer = new StringBuilder();
    buffer.append(
      "Dataset,syncInterval,repetition,original file size(in bytes),compressed file size(in bytes),formatted original file size,formatted compressed file size,read time (in ms),write time (in ms),codec,number of occurrence\n");
    for (CompressionResult result : compressionResults) {
      buffer.append(result.toCSV()).append("\n");
    }
    Files.write(new File(resultPath).toPath(), buffer.toString().getBytes(StandardCharsets.UTF_8));
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      LOG.error(
        "Usage java -jar labs.jar org.gbif.pipelines.labs.performance.AvroCompressionTestUtility /path/to/dataset /path/to/result.csv 2");
      System.exit(1);
    }
    Integer[] syncIntervals = new Integer[] {128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2048 * 1024};
    runCompressionTest(args[0], args[1], Integer.parseInt(args[2]), syncIntervals);
  }

}
