package org.gbif.converters.converter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConverterToVerbatim {

  private static final Logger LOG = LoggerFactory.getLogger(ConverterToVerbatim.class);

  private String hdfsSiteConfig;
  private int syncInterval = 2 * 1024 * 1024;
  private CodecFactory codecFactory = CodecFactory.snappyCodec();

  private java.nio.file.Path inputPath;
  private Path outputPath;
  private Path metaPath;

  public ConverterToVerbatim hdfsSiteConfig(String hdfsSiteConfig) {
    this.hdfsSiteConfig = hdfsSiteConfig;
    return this;
  }

  public ConverterToVerbatim syncInterval(int syncInterval) {
    this.syncInterval = syncInterval;
    return this;
  }

  public ConverterToVerbatim codecFactory(CodecFactory codecFactory) {
    this.codecFactory = codecFactory;
    return this;
  }

  public ConverterToVerbatim outputPath(Path outputPath) {
    this.outputPath = outputPath;
    return this;
  }

  public ConverterToVerbatim inputPath(java.nio.file.Path inputPath) {
    this.inputPath = inputPath;
    return this;
  }

  public ConverterToVerbatim metaPath(Path metaPath) {
    this.metaPath = metaPath;
    return this;
  }

  public ConverterToVerbatim outputPath(String outputPath) {
    this.outputPath = new Path(outputPath);
    return this;
  }

  public ConverterToVerbatim inputPath(String inputPath) {
    this.inputPath = Paths.get(inputPath);
    return this;
  }

  public ConverterToVerbatim metaPath(String metaPath) {
    this.metaPath = new Path(metaPath);
    return this;
  }

  public boolean convert() {

    Objects.requireNonNull(inputPath, "inputPath cannot be null");
    Objects.requireNonNull(outputPath, "outputPath cannot be null");

    boolean isConverted;

    // the fs has to be out of the try-catch block to avoid closing it, because the hdfs client
    // tries to reuse the
    // same connection. So, when using multiple consumers, one consumer would close the connection
    // that is being used
    // by another consumer.
    FileSystem fs = FsUtils.createParentDirectories(outputPath, hdfsSiteConfig);
    try (BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(outputPath));
        DataFileWriter<ExtendedRecord> dataFileWriter =
            DataFileWriteBuilder.create()
                .schema(ExtendedRecord.getClassSchema())
                .codec(codecFactory)
                .outputStream(outputStream)
                .syncInterval(syncInterval)
                .build()) {

      long numberOfRecords = convert(inputPath, dataFileWriter);

      createMetafile(fs, metaPath, numberOfRecords);

    } catch (IOException e) {
      LOG.error("Failed performing conversion on {}", inputPath, e);
      throw new IllegalStateException("Failed performing conversion on " + inputPath, e);
    } finally {
      isConverted = FsUtils.deleteAvroFileIfEmpty(fs, outputPath);
    }

    return !isConverted;
  }

  private void createMetafile(FileSystem fs, Path metaPath, long numberOfRecords) throws IOException {
    if (metaPath != null) {
      String info = "dwcaToAvroCount: " + numberOfRecords + "\n";
      FsUtils.createFile(fs, metaPath, info);
    }
  }

  protected abstract long convert(java.nio.file.Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter)
      throws IOException;
}
