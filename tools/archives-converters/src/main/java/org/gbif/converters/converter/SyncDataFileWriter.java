package org.gbif.converters.converter;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;

import lombok.AllArgsConstructor;

/** Sync class for avro DataFileWriter, created to avoid an issue during file writing */
@AllArgsConstructor
public class SyncDataFileWriter<T> implements Closeable {

  private final DataFileWriter<T> dataFileWriter;

  /** Synchronized append method, helps avoid the ArrayIndexOutOfBoundsException */
  public synchronized void append(T record) {
    try {
      dataFileWriter.append(record);
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  @Override
  public void close() throws IOException {
    dataFileWriter.close();
  }
}
