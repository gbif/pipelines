package org.gbif.converters.converter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import lombok.Builder;
import lombok.NonNull;

@Builder
public class DataFileWriteBuilder {

  @NonNull
  private Schema schema;
  @NonNull
  private CodecFactory codec;
  @NonNull
  private OutputStream outputStream;
  private Integer syncInterval;
  private Boolean flushOnEveryBlock;

  public <T> DataFileWriter<T> createDataFileWriter() throws IOException {
    DataFileWriter<T> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));

    dataFileWriter.setCodec(codec);
    Optional.ofNullable(flushOnEveryBlock).ifPresent(dataFileWriter::setFlushOnEveryBlock);
    Optional.ofNullable(syncInterval).ifPresent(dataFileWriter::setSyncInterval);
    dataFileWriter.create(schema, outputStream);

    return dataFileWriter;
  }
}
