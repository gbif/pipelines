package org.gbif.converters.converter;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

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

  public boolean convert(String inputPath, String outputPath) {
    return convert(Paths.get(inputPath), new Path(outputPath));
  }

  public boolean convert(java.nio.file.Path inputPath, Path outputPath) {

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

      convert(inputPath, dataFileWriter);

    } catch (IOException e) {
      LOG.error("Failed performing conversion on {}", inputPath, e);
      throw new IllegalStateException("Failed performing conversion on " + inputPath, e);
    } finally {
      isConverted = FsUtils.deleteAvroFileIfEmpty(fs, outputPath);
    }

    return !isConverted;
  }

  protected abstract void convert(
      java.nio.file.Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter)
      throws IOException;
}
