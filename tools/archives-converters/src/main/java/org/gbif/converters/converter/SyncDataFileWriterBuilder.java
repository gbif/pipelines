package org.gbif.converters.converter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

@Builder
public class SyncDataFileWriterBuilder {

  @NonNull private Schema schema;
  @NonNull private String codec;
  @NonNull private OutputStream outputStream;
  private Integer syncInterval;
  private Boolean flushOnEveryBlock;

  public <T> SyncDataFileWriter<T> createSyncDataFileWriter() throws IOException {
    DataFileWriter<T> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));

    dataFileWriter.setCodec(CodecFactory.fromString(codec));
    Optional.ofNullable(flushOnEveryBlock).ifPresent(dataFileWriter::setFlushOnEveryBlock);
    Optional.ofNullable(syncInterval).ifPresent(dataFileWriter::setSyncInterval);
    dataFileWriter.create(schema, new BufferedOutputStream(outputStream));

    return new SyncDataFileWriter<>(dataFileWriter);
  }
}
