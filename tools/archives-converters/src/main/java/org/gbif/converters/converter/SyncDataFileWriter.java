package org.gbif.converters.converter;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/** Sync class for avro DataFileWriter, created to avoid an issue during file writing */
@AllArgsConstructor
public class SyncDataFileWriter<T> implements Closeable {

  private final DataFileWriter<T> dataFileWriter;

  /** Synchronized append method, helps avoid the ArrayIndexOutOfBoundsException */
  @SneakyThrows
  public synchronized void append(T record) {
    dataFileWriter.append(record);
  }

  @Override
  public void close() throws IOException {
    dataFileWriter.close();
  }
}
