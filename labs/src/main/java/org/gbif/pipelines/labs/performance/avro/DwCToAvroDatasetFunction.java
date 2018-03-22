package org.gbif.pipelines.labs.performance.avro;

import org.gbif.pipelines.labs.performance.CompressionRequest;
import org.gbif.pipelines.labs.performance.CompressionResult;
import org.gbif.pipelines.core.io.DwCAReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataset Function to run test for read,write, compression size of avro file generated from expanded DwC.
 */
public class DwCToAvroDatasetFunction implements Function<CompressionRequest, CompressionResult> {

  private static final Logger LOG = LoggerFactory.getLogger(DwCToAvroDatasetFunction.class);

  private CompressionResult result;
  private Path dumpedFilePath;
  private int noOfOccurrence = 0;
  private int syncInterval;
  private CodecFactory codec;

  public CompressionResult apply(CompressionRequest compressionRequest) {
    result = new CompressionResult(compressionRequest.getDataset(),
                                   compressionRequest.getSyncInterval(),
                                   FileUtils.sizeOfDirectory(compressionRequest.getDataset().toFile()),
                                   compressionRequest.getCodec());
    dumpedFilePath = Paths.get(compressionRequest.getDataset().toUri().getPath(),
                               "result"
                               + compressionRequest.getSyncInterval()
                               + "-"
                               + compressionRequest.getCodec()
                               + ".avro");
    LOG.warn("Performance request " + compressionRequest + " started");
    for (int i = 0; i < compressionRequest.getRepetition(); i++) {
      LOG.warn("Repetition " + (i + 1) + " started");

      long writeTime = performWriteTest(compressionRequest.getDataset(),
                                        compressionRequest.getSyncInterval(),
                                        compressionRequest.getCodec());
      long compressedSize = dumpedFilePath.toFile().length();
      long readTime = performReadTest();
      try {
        Files.deleteIfExists(dumpedFilePath);
      } catch (IOException e) {

      }
      result.updateReadings(readTime, writeTime, compressedSize);
      result.setNoOfOccurrence(noOfOccurrence);
      noOfOccurrence=0;
    }
    LOG.warn("Result of request: " + result.toCSV());
    return result;
  }

  /**
   * Performs read on generated avro file and returns time taken to read file.
   */
  private long performReadTest() {
    long startTime = System.currentTimeMillis();
    try (DataFileReader<ExtendedRecord> dataFileReader = new DataFileReader<>(dumpedFilePath.toFile(),
                                                                              new SpecificDatumReader<ExtendedRecord>(
                                                                                ExtendedRecord.class))) {
      ExtendedRecord record = null;
      while (dataFileReader.hasNext()) {
        record = dataFileReader.next(record);
        ++noOfOccurrence;
      }
    } catch (IOException e) {

    }
    long runTime = System.currentTimeMillis() - startTime;
    return runTime;
  }
  /**
   * Performs write converting dwca to avro returns time taken to write file.
   */
  private long performWriteTest(Path dataset, int syncInterval, CodecFactory codec) {
    long startTime = System.currentTimeMillis();
    try (
      DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<ExtendedRecord>())
        .setCodec(codec)
        .setSyncInterval(syncInterval)
        .create(ExtendedRecord.getClassSchema(), new FileOutputStream(dumpedFilePath.toFile()))) {

      DwCAReader reader = new DwCAReader(dataset.toUri().getPath());
      reader.init();
      while (reader.advance()) {
        dataFileWriter.append(reader.getCurrent());
      }

    } catch (IOException e) {
      throw new IllegalStateException("Failed performing conversion on " + dataset.toUri().getPath());
    }
    long runTime = System.currentTimeMillis() - startTime;
    return runTime;
  }
}
