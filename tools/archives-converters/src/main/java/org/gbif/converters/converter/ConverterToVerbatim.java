package org.gbif.converters.converter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@Slf4j
public abstract class ConverterToVerbatim {

  private String hdfsSiteConfig;
  private String coreSiteConfig;
  private int syncInterval = 2 * 1024 * 1024;
  private CodecFactory codecFactory = CodecFactory.snappyCodec();

  private java.nio.file.Path inputPath;
  private Path outputPath;
  private Path metaPath;

  public ConverterToVerbatim hdfsSiteConfig(String hdfsSiteConfig) {
    this.hdfsSiteConfig = hdfsSiteConfig;
    return this;
  }

  public ConverterToVerbatim coreSiteConfig(String coreSiteConfig) {
    this.coreSiteConfig = coreSiteConfig;
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
    FileSystem fs = FsUtils.createParentDirectories(hdfsSiteConfig, coreSiteConfig, outputPath);
    try (BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(outputPath));
        SyncDataFileWriter<ExtendedRecord> dataFileWriter =
            SyncDataFileWriterBuilder.builder()
                .schema(ExtendedRecord.getClassSchema())
                .codec(codecFactory.toString())
                .outputStream(outputStream)
                .syncInterval(syncInterval)
                .build()
                .createSyncDataFileWriter()) {

      long numberOfRecords = convert(inputPath, dataFileWriter);

      createMetafile(fs, metaPath, numberOfRecords);

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", inputPath, e);
      throw new IllegalStateException("Failed performing conversion on " + inputPath, e);
    } finally {
      isConverted = FsUtils.deleteAvroFileIfEmpty(fs, outputPath);
    }

    return !isConverted;
  }

  private void createMetafile(FileSystem fs, Path metaPath, long numberOfRecords)
      throws IOException {
    if (metaPath != null) {
      String info = Metrics.ARCHIVE_TO_ER_COUNT + ": " + numberOfRecords + "\n";
      FsUtils.createFile(fs, metaPath, info);
    }
  }

  protected abstract long convert(
      java.nio.file.Path inputPath, SyncDataFileWriter<ExtendedRecord> dataFileWriter)
      throws IOException;
}
