package org.gbif.pipelines.core.io;

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

  @NonNull private final Schema schema;
  @NonNull private final String codec;
  @NonNull private final OutputStream outputStream;
  private final Integer syncInterval;
  private final Boolean flushOnEveryBlock;

  public <T> SyncDataFileWriter<T> createSyncDataFileWriter() throws IOException {
    DataFileWriter<T> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));

    dataFileWriter.setCodec(CodecFactory.fromString(codec));
    Optional.ofNullable(flushOnEveryBlock).ifPresent(dataFileWriter::setFlushOnEveryBlock);
    Optional.ofNullable(syncInterval).ifPresent(dataFileWriter::setSyncInterval);
    dataFileWriter.create(schema, new BufferedOutputStream(outputStream));

    return new SyncDataFileWriter<>(dataFileWriter);
  }
}
