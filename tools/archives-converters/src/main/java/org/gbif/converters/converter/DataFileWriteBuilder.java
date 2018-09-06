package org.gbif.converters.converter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class DataFileWriteBuilder {

  private Schema schema;
  private CodecFactory codec;
  private Integer syncInterval;
  private Boolean flushOnEveryBlock;
  private OutputStream outputStream;

  private DataFileWriteBuilder() {}

  public static DataFileWriteBuilder create() {
    return new DataFileWriteBuilder();
  }

  public DataFileWriteBuilder schema(Schema schema) {
    Objects.requireNonNull(schema, "Schema can't be NULL");
    this.schema = schema;
    return this;
  }

  public DataFileWriteBuilder outputStream(OutputStream outputStream) {
    Objects.requireNonNull(outputStream, "OutputStream can't be NULL");
    this.outputStream = outputStream;
    return this;
  }

  public DataFileWriteBuilder codec(CodecFactory codec) {
    Objects.requireNonNull(codec, "CodecFactory can't be NULL");
    this.codec = codec;
    return this;
  }

  public DataFileWriteBuilder syncInterval(Integer syncInterval) {
    this.syncInterval = syncInterval;
    return this;
  }

  public DataFileWriteBuilder flushOnEveryBlock(Boolean flushOnEveryBlock) {
    this.flushOnEveryBlock = flushOnEveryBlock;
    return this;
  }

  public <T> DataFileWriter<T> build() throws IOException {
    DataFileWriter<T> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));

    dataFileWriter.setCodec(codec);
    Optional.ofNullable(flushOnEveryBlock).ifPresent(dataFileWriter::setFlushOnEveryBlock);
    Optional.ofNullable(syncInterval).ifPresent(dataFileWriter::setSyncInterval);
    dataFileWriter.create(schema, outputStream);

    return dataFileWriter;
  }
}
